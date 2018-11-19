from __future__ import print_function

import json
import logging
import os
import re
import time
from datetime import tzinfo, timedelta, datetime

from boto3 import client
from botocore.exceptions import ClientError

"""

This Lambda function when deployed using the AWS SAM template 'rds_copy_snap_template.yaml' will be part of the
'RDS Snapshot Copy Stack'. When run using the default AWS Lambda payload, this function will make a manual copy of
the most recent automated snapshot for one or more RDS instances. It then shares the snapshot with a
'restricted' Failsafe account, sends an SNS notification to the subscription Topic.

"""

FAILSAFE_TAG = 'failsafe'  # this tag has to be present on the rds instance to backup to failsafe account
FAILSAFE_SNAPSHOT_PREFIX = 'failsafe-'
SNS_RDS_SAVE_TOPIC = 'reptileinx_save_failsafe_snapshot_sns_topic'
AWS_DEFAULT_REGION = 'ap-southeast-2'
FAILSAFE_ACCOUNT_ID = os.environ['FAILSAFE_ACCOUNT_ID']
MANUAL_SNAPSHOT_EXISTS_MESSAGE = 'Manual snapshot already exists for the automated snapshot {}'
ZERO = timedelta(0)  # required to handle timezones correctly


class UTC(tzinfo):
    def __init__(self, dt):
        return 'UTC' if self.tzname else ZERO
    # def utcoffset(self, dt):
    #     return ZERO
    #
    # def tzname(self, dt):
    #     return 'UTC'
    #
    # def dst(self, dt):
    #     return ZERO


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClientException(Exception):
    pass


def create_failsafe_manual_snapshot(rds, instance):
    """
    Checks the database instance has got a recent automated snapshot created. Creates a copy of the automated snapshot
    to a manual snapshot. The manual snapshot is allocated a unique name. The created manual snapshot is modified
    such that it can be shared with the account passed in as ACCOUNT_TO_SHARE_WITH.

    :param rds: instantiated boto3 object
    :param instance: name of database instance to copy snapshot from
    :return:
        - None: if a copy of the automated snapshot has been created already
        - Snapshot: dictionary payload of the snapshot successfully copied
    """

    logger.info('Creating manual copy of the most recent automated snapshot of database instance - {}'.format(instance))
    name_of_newest_automated_snapshot = get_name_of_newest_automated_snapshot(instance, rds)
    name_of_created_failsafe_snapshot = create_name_of_failsafe_snapshot(name_of_newest_automated_snapshot,
                                                                         FAILSAFE_SNAPSHOT_PREFIX)
    manual_snapshots = get_snapshots(rds, instance, 'manual')
    for manual_snapshot in manual_snapshots:
        if manual_snapshot['DBSnapshotIdentifier'] == name_of_created_failsafe_snapshot:
            logger.warn(MANUAL_SNAPSHOT_EXISTS_MESSAGE.format(name_of_newest_automated_snapshot))
            return name_of_created_failsafe_snapshot
    else:
        return perform_copy_automated_snapshot(instance, name_of_created_failsafe_snapshot,
                                               name_of_newest_automated_snapshot, rds)


def perform_copy_automated_snapshot(instance, name_of_created_failsafe_snapshot,
                                    name_of_newest_automated_snapshot,
                                    rds):
    """
    Where the actual copying of the automated snapshot actually happens. If copy successfully completes
    the automated_snapshot_copied flag is update and sent to the main function for further use.
    The function was mainly pulled out to help with testing the unimplemented method rds.copy_db_snapshot in moto.
    Otherwise errors are bubbled up to be handled by a generic try catch all. AWS will record any logs.
    :param instance: DB instance with an automated snapshot that is being copied
    :param name_of_created_failsafe_snapshot: the resulting name of the failsafe manual snapshot
    :param name_of_newest_automated_snapshot: the name of the newest automated snapshot being copied
    :param rds: the Boto3 client with the help of which we interrogate AWS RDS services
    :return: Name of Failsafe snapshot or empty string
    """
    if name_of_newest_automated_snapshot:
        response = rds.copy_db_snapshot(
            SourceDBSnapshotIdentifier=name_of_newest_automated_snapshot,
            TargetDBSnapshotIdentifier=name_of_created_failsafe_snapshot
        )
        wait_until_failsafe_snapshot_is_available(rds, instance, name_of_created_failsafe_snapshot)
        logger.info('Snapshot {} copied to {}'.format(name_of_newest_automated_snapshot,
                                                      name_of_created_failsafe_snapshot))
        return response.get('DBSnapshot', {}).get('DBSnapshotIdentifier', '')


def create_name_of_failsafe_snapshot(name_of_newest_automated_snapshot, failsafe_snapshot_name_prefix):
    name_of_created_failsafe_snapshot = failsafe_snapshot_name_prefix + name_of_newest_automated_snapshot[4:]
    return name_of_created_failsafe_snapshot


def get_name_of_newest_automated_snapshot(instance, rds):
    automated_snapshots = get_snapshots(rds, instance, 'automated')
    newest_automated_snapshot = automated_snapshots[-1]
    name_of_newest_automated_snapshot = newest_automated_snapshot['DBSnapshotIdentifier']
    return name_of_newest_automated_snapshot


def send_sns_to_failsafe_account(instance, name_of_created_failsafe_snapshot):
    """
    Sends an SNS notification to the subscribed Lambda function.
    The notification contains the Failsafe snapshot payload:
    {'Instance': instance, 'FailsafeSnapshotID': name_of_created_failsafe_snapshot}
    :param instance: DB instance with an automated snapshot that is being copied
    :param name_of_created_failsafe_snapshot: name of Failsafe Snapshot to be shared
    :return: None
    """
    failsafe_sns_save_topic_arn = get_subscription_sns_topic_arn()
    if failsafe_sns_save_topic_arn:
        logger.info('Sending SNS alert to failsafe topic - {}'.format(failsafe_sns_save_topic_arn))
        failsafe_notification_payload = {'Instance': instance, 'FailsafeSnapshotID': name_of_created_failsafe_snapshot}
        logger.warn('message sent: {}'.format(failsafe_notification_payload))
        sns = client('sns', region_name=AWS_DEFAULT_REGION)
        sns.publish(
            TargetArn=failsafe_sns_save_topic_arn,
            Message=json.dumps({'default': json.dumps(failsafe_notification_payload)}),
            MessageStructure='json'
        )


def share_failsafe_snapshot(rds, name_of_failsafe_snapshot):
    """
    Shares the Failsafe snapshot with the Backup account
    :param rds: the Boto3 client with the help of which we interrogate AWS RDS services
    :param name_of_failsafe_snapshot: name of Failsafe Snapshot to be shared
    :return: None
    """
    if FAILSAFE_ACCOUNT_ID:
        logger.info(
            'Sharing snapshot... {} to account ... {} '.format(name_of_failsafe_snapshot, FAILSAFE_ACCOUNT_ID))
        logger.warn('Security Notice: DB Snapshot {0} will remain shared to {1} until when snapshot is deleted'
                    .format(name_of_failsafe_snapshot, FAILSAFE_ACCOUNT_ID))
        rds.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=name_of_failsafe_snapshot,
            AttributeName='restore',
            ValuesToAdd=[
                FAILSAFE_ACCOUNT_ID
            ]
        )


def wait_until_failsafe_snapshot_is_available(rds, instance, failsafe_snapshot):
    """
    A function that allows the lambda function to wait for long running events to complete.
    This allows us to have more control on the overall workflow of RDS Snapshot Backups
    :param rds: the Boto3 client with the help of which we interrogate AWS RDS services
    :param instance: name of database instance to copy snapshot from
    :param failsafe_snapshot: name of the Failsafe snapshot being created
    :return: None
    """
    logger.info('Waiting for copy of {} to complete.'.format(failsafe_snapshot))
    available = False
    while not available:
        time.sleep(10)
        manual_snapshots = get_snapshots(rds, instance, 'manual')
        for manual_snapshot in manual_snapshots:
            if manual_snapshot['DBSnapshotIdentifier'] == failsafe_snapshot:
                logger.info('{}: {}...'.format(manual_snapshot['DBSnapshotIdentifier'], manual_snapshot['Status']))
                if manual_snapshot['Status'] == 'available':
                    available = True
                    break


def delete_old_failsafe_manual_snapshots(rds, instance):
    """
    Deletes any previously created failsafe manual snapshots. Failsafe manual snapshot here being a copy of the
    automated snapshot that has been shared with the Failsafe account. This is a security feature to ensure
    snapshots are not shared for periods more than 48 hours.

    :param rds: the Boto3 client with the help of which we interrogate AWS RDS services
    :param instance: name of database instance to copy snapshot from
    :return: None
    """
    logger.info('Preparing deletion of previously created manual snapshots for DB instance - {}'.format(instance))
    manual_snapshots = get_snapshots(rds, instance, 'manual')
    for manual_snapshot in manual_snapshots:
        snapshot_id_prefix_is_not_failsafe = manual_snapshot['DBSnapshotIdentifier'][:9] != 'failsafe-'
        if snapshot_id_prefix_is_not_failsafe:
            logger.info('Ignoring manual snapshot {}'.format(manual_snapshot['DBSnapshotIdentifier']))
            continue
        logger.info('Deleting previously created manual snapshot - {}'.format(manual_snapshot['DBSnapshotIdentifier']))
        rds.delete_db_snapshot(
            DBSnapshotIdentifier=manual_snapshot['DBSnapshotIdentifier']
        )


def get_snapshot_date(snapshot):
    """
    This is a helper function to ascertain snapshot has completed creating.
    When SnapshotCreateTime is present then the snapshot has finished creating
    :param snapshot: snapshot being created
    :return: datetime value of when snapshot was created
    """
    return datetime.now(UTC()) if snapshot['Status'] != 'available' else snapshot['SnapshotCreateTime']


def get_snapshots(rds, instance, snapshot_type):
    """
    Gets a sorted list automated or manual snapshots depepnding on the snapshot_type value
    :param rds: the Boto3 client with the help of which we interrogate AWS RDS services
    :param instance: the specific instance to get snapshots from
    :param snapshot_type: can be 'automated' or 'manual'
    :return: sorted list of snapshots
    """
    # TODO: refactor call this api once
    if instance:
        snapshots = rds.describe_db_snapshots(
            SnapshotType=snapshot_type,
            DBInstanceIdentifier=instance,
            IncludeShared=True)['DBSnapshots']
    else:
        snapshots = rds.describe_db_snapshots(
            SnapshotType=snapshot_type,
            IncludeShared=True)['DBSnapshots']
    if snapshots is not None:
        sorted_snapshots = sorted(snapshots, key=get_snapshot_date)
    return sorted_snapshots


def get_subscription_sns_topic_arn():
    """
    Helper function to get the SNS Topic arn.
    :return: sns topic arn
    """
    sns = client('sns', region_name=AWS_DEFAULT_REGION)
    sns_topic_list = sns.list_topics().get('Topics', [])
    for sns_topic in sns_topic_list:
        if not re.search(SNS_RDS_SAVE_TOPIC, sns_topic['TopicArn']):
            continue
        else:
            failsafe_sns_topic_arn = sns_topic['TopicArn']
            logger.info('Setting failsafe topic arn to - {}'.format(failsafe_sns_topic_arn))
            return failsafe_sns_topic_arn
    logger.error('Initial setup required. Failsafe SNS topic {} not found.'.format(SNS_RDS_SAVE_TOPIC))


def event_guard(event):
    for record in event['Records']:
        if record['EventSource'] == 'aws:sns' and record['Sns']['Message']:
            event_id_raw = json.loads(json.dumps(record['Sns']['Message']))['Event ID']
            event_id = re.findall(r'#(.*)', event_id_raw)[0].encode('ascii')
            logger.info('received event {} from RDS'.format(event_id))
            if event_id != 'RDS-EVENT-0002':
                raise ClientException('received an event not suitable for backup...')


def run_rds_snapshot_backup(instance):
    """
    The function that AWS Lambda service invokes when executing the code in this module.
    :param instance: instance that triggered the Copy SNS Topic
    :return: true if an automated snapshot was copied, shared and a notification was sent to an SNS Topic
    """
    if instance:
        try:
            rds = client('rds', region_name=AWS_DEFAULT_REGION)
            delete_old_failsafe_manual_snapshots(rds, instance)
            name_of_created_failsafe_snapshot = create_failsafe_manual_snapshot(rds, instance)
            if name_of_created_failsafe_snapshot:
                share_failsafe_snapshot(rds, name_of_created_failsafe_snapshot)
                send_sns_to_failsafe_account(instance, name_of_created_failsafe_snapshot)
        except ClientError as e:
            logger.error(str(e))
    else:
        raise ClientException('No instances tagged for RDS failsafe backup have been found...')


def get_db_instances_from_notification(event):
    for record in event['Records']:
        db_instance = json.loads(json.dumps(record['Sns']['Message']))['Source ID']
        return db_instance


def handler(event, context):
    """
    The function that AWS Lambda service invokes when executing the code in this module.
    :param event: used to to pass in event data to the handler. an RDS notification will trigger this process
    :param context: we are not providing any runtime information to the handler if required
    :return: true if an automated snapshot was copied, shared and a notification was sent to an SNS Topic
    """
    event_guard(event)
    db_instance = get_db_instances_from_notification(event)
    run_rds_snapshot_backup(db_instance)
