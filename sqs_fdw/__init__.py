import json
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
import boto3


class SQSForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(SQSForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.queue_url = options['queue_url']

        log_to_postgres('Connecting to {}'.format(self.queue_url))
        # log_to_postgres(str(columns))

        if 'message_attributes' in columns:
            if not 'json' in columns['message_attributes'].base_type_name:
                log_to_postgres("The message_attributes column must be of json(b) type", ERROR)
            self.message_attribute_names = [x.strip() for x in options['message_attribute_names'].split(',')]
        else:
            self.message_attribute_names = []

        try:
            if 'aws_profile' in options:
                session = boto3.Session(
                    profile_name=options['aws_profile'],
                    region_name=options['aws_region'],
                )
                self.client = session.client('sqs')
            else:
                self.client = boto3.client(
                    'sqs',
                    region_name=options['aws_region'],
                    aws_access_key_id=options['aws_access_key_id'],
                    aws_secret_access_key=options['aws_secret_access_key'],
                )
        except KeyError as e:
            log_to_postgres("An error occurred creating the FDW: " + str(e), ERROR)

    def execute(self, quals, columns):
        # log_to_postgres(str(quals))
        # log_to_postgres(str(columns))
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            # AttributeNames=['All'],
            MessageAttributeNames=self.message_attribute_names,
            MaxNumberOfMessages=10,
            # VisibilityTimeout=123,
            # WaitTimeSeconds=123,
        )

        if not 'Messages' in response:
            return

        log_to_postgres('Received from SQS ({}) - {}'.format(self.queue_url, response), DEBUG)
        receipt_handles = []

        for msg in response['Messages']:
            body = json.loads(msg['Body'])
            line = {}
            if 'message_id' in columns:
                line['message_id'] = msg['MessageId']
            if self.message_attribute_names and 'MessageAttributes' in msg:
                line['message_attributes'] = json.dumps(msg['MessageAttributes'])
            for column_name in columns:
                if column_name not in ('message_id', 'message_attributes'):
                    line[column_name] = body.get(column_name)
            # log_to_postgres(str(line))
            receipt_handles.append(msg['ReceiptHandle'])
            yield line

        # ideally we would only make this call on commit, by we don't have access to the ReceiptHandle then.
        self.client.delete_message_batch(
            QueueUrl=self.queue_url,
            Entries=[
                {'Id': 'msg-{}'.format(i), 'ReceiptHandle': handle}
                for i, handle in enumerate(receipt_handles)
            ]
        )

    def get_rel_size(self, quals, columns):
        # FIXME estimate rows based on queue length
        # FIXME calculate bytes based on columns
        return 1, 100

    @property
    def rowid_column(self):
        return 'message_id'

    def insert(self, new_values):
        #log_to_postgres(str(new_values))
        self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(new_values)
        )
