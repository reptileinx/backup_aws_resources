"""
    A Lambda saves the most recently shared failsafe RDS manual snapshot to the
    Failsafe account. The RDS Instance should be tagged with 'Failsafe=true'
    to get its snapshots backed up. The Lambda will be extended to send a
    success message to a slack channel in future. Use the AWS SAM Templates
    (rds_save_snap_template) provided to deploy this function. This function
    depends on the Resources created by the rds_copy_snap_template

    FAILSAFE_TAG: this tag has to put on the target DB for its snapshots
    to be backed up to the Failsafe Account
"""
from __future__ import print_function

import json
import logging
import re
import time
from datetime import tzinfo, timedelta, datetime

from boto3 import client
from botocore.exceptions import ClientError

SERVICE_CONNECTION_DEFAULT_REGION = "ap-southeast-2"
FAILSAFE_TAG = 'failsafe'
SNAPSHOT_RETENTION_PERIOD_IN_DAYS = 31
ZERO = timedelta(0)  # Handle timezones correctly
TESTING_HACK = False

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ClientException(Exception):
    pass


class UTC(tzinfo):
    """
        To help with formatting date/time
    """

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()


def terminate_copy_manual_failsafe_snapshot():
    logger.warn('No shared snapshots found.')
    raise ClientException('No shared snapshots found.')


def copy_manual_failsafe_snapshot_and_save(rds,
                                           instance,
                                           failsafe_snapshot_id):
    """
    Function discovers the shared snapshot and copies it to the failsafe
    snasphot
    :param rds: the Boto3 client which use to interrogate AWS RDS services
    :param instance: rds db snapshot we save to the failsafe account
    :param failsafe_snapshot_id: the identifier of the
    failsafe snapshot to be created
    :return:
    """
    logger.info('Making local copy of {} in Failsafe account'
                .format(failsafe_snapshot_id))
    manual_snapshots = get_snapshots(rds,
                                     db_instance_id=instance,
                                     snapshot_type='manual')
    shared_snapshots = get_snapshots(rds,
                                     db_instance_id='',
                                     snapshot_type='shared')
    if not shared_snapshots:
        terminate_copy_manual_failsafe_snapshot()

    shared_snapshot_id = ''.join(
        [shared_snapshot['DBSnapshotIdentifier']
            for shared_snapshot in shared_snapshots
         for shared_snapshot_arn in [re.search(
                                    failsafe_snapshot_id,
                                    shared_snapshot['DBSnapshotIdentifier'])]
         if shared_snapshot_arn])

    snapshot_copied = [
        data_of_copied_snapshot(failsafe_snapshot_id,
                                instance,
                                manual_snapshots,
                                rds,
                                shared_snapshot_id)
        if match_shared_snapshot_requiring_copy(failsafe_snapshot_id,
                                                shared_snapshot_id) else None]

    if not snapshot_copied.pop():
        logger.error('Shared snapshot with id ...:snapshot:{} failed to copy.'
                     .format(failsafe_snapshot_id))


def data_of_copied_snapshot(failsafe_snapshot_id,
                            instance,
                            manual_snapshots,
                            rds,
                            shared_snapshot_id):
    logger.info('Failsafe Snapshot {} matched successfully'
                .format(shared_snapshot_id))
    delete_duplicate_snapshots(failsafe_snapshot_id,
                               manual_snapshots, rds)
    snapshot_copied = copy_failsafe_snapshot(failsafe_snapshot_id,
                                             instance,
                                             rds,
                                             shared_snapshot_id)
    return snapshot_copied


def copy_failsafe_snapshot(failsafe_snapshot_id,
                           instance,
                           rds,
                           shared_snapshot_id):
    """
    Performs copy of the shared manual snapshot to the failsafe manual
    snapshot and saves it
    :param failsafe_snapshot_id: the identifier of the failsafe snapshot
    provided
    :param instance: the instance of the database whose snapshot will
    be backed-up
    :param rds: the Boto3 client with the help of which we interrogate
    AWS RDS services
    :param shared_snapshot_id: the identifier of the snapshot being copied
    :return: payload of the copied snapshot
    """
    response = rds.copy_db_snapshot(
        SourceDBSnapshotIdentifier=shared_snapshot_id,
        TargetDBSnapshotIdentifier=failsafe_snapshot_id
    )
    wait_until_snapshot_is_available(rds, instance, failsafe_snapshot_id)
    logger.info("Snapshot {} copied to {}"
                .format(shared_snapshot_id, failsafe_snapshot_id))
    return response


def delete_duplicate_snapshots(failsafe_snapshot_id, manual_snapshots, rds):
    """
    Helper function to delete snapshots whose creation is being repeated.
    The failsafe snapshot already exists but the rdssavesnapshot lambda
    has been invoked
    :param failsafe_snapshot_id:
    :param manual_snapshots:
    :param rds: the Boto3 client with the help of which we interrogate
    AWS RDS services
    :return:
    """
    logger.warn("Initiating duplicate snapshot cleanup...")
    if local_snapshot_deletion_required(failsafe_snapshot_id,
                                        manual_snapshots):
        perform_delete(failsafe_snapshot_id, rds)
    logger.info("Duplicate snapshot cleanup successfully complete")
    return


def perform_delete(failsafe_snapshot_id, rds):
    rds.delete_db_snapshot(
        DBSnapshotIdentifier=failsafe_snapshot_id
    )


def match_shared_snapshot_requiring_copy(failsafe_snapshot_id,
                                         shared_snapshot_identifier):
    """
    Helper function to find which shared snapshot requires copying using
    a string matcher
    :param failsafe_snapshot_id: Failsafe snapshot id from the SNS event
    :param shared_snapshot_identifier: Shared snapshot id being matched for
    copy in failsafe account
    :return: a match object with boolean value of True if there is a match,
    and None if not.
    """
    logger.info("Checking if snapshot {} requires copying"
                .format(shared_snapshot_identifier))
    regexp = r".*\:{}".format(re.escape(failsafe_snapshot_id))
    return re.match(regexp, shared_snapshot_identifier)


def local_snapshot_deletion_required(failsafe_snapshot_id, manual_snapshots):
    """
    Helper function that runs before every copy snapshot invocation.
    This function will delete any previously created
    failsafe snapshot and create a new one in its place.
    :param failsafe_snapshot_id: Failsafe snapshot ID that will be created
    :param manual_snapshots: Shared manual snapshot requiring backup to
    failsafe account
    :return:
    """

    failsafe_snapshot_id_exists = [manual_snapshot
                                   for manual_snapshot in manual_snapshots if
                                   manual_snapshot['DBSnapshotIdentifier'] ==
                                   failsafe_snapshot_id]
    if not failsafe_snapshot_id_exists:
        return False
    if failsafe_snapshot_id_exists.pop():
        logger.warn(
               'Local copy of {} already exists - deleting it before copying'
               .format(manual_snapshot['DBSnapshotIdentifier']))
        return True


def wait_until_snapshot_is_available(rds, instance, snapshot):
    """
    A function that allows the lambda function to wait for long running events
    to complete. This allows us to have more control on the overall workflow of
    RDS Snapshot Backups
    :param rds: the Boto3 client used to interrogate AWS RDS services
    :param instance: name of database instance to copy snapshot from
    :param snapshot: name of the Failsafe snapshot being created
    :return: None
    """
    logger.info("Waiting for copy of {} to complete.".format(snapshot))
    available = False
    while not available:
        time.sleep(10)
        manual_snapshots = get_snapshots(rds,
                                         db_instance_id=instance,
                                         snapshot_type='manual')
        for manual_snapshot in manual_snapshots:
            if manual_snapshot['DBSnapshotIdentifier'] == snapshot:
                logger.info("{}: {}..."
                            .format(manual_snapshot['DBSnapshotIdentifier'],
                                    manual_snapshot['Status']))
                if manual_snapshot['Status'] == "available":
                    available = True
                    break


def delete_old_failsafe_manual_snapshots(rds, instance):
    """
    Deletes expired snapshots in accordance with the retention policy
    :param rds: the Boto3 client used interrogate AWS RDS services
    :param instance:
    :return:
    """
    logger.info("Checking if instance {} has expired snapshots "
                .format(instance))
    logger.warn("Manual snapshots older than {} days will be deleted."
                .format(SNAPSHOT_RETENTION_PERIOD_IN_DAYS))
    manual_snapshots = get_snapshots(rds,
                                     db_instance_id=instance,
                                     snapshot_type='manual')
    for manual_snapshot in manual_snapshots:
        if manual_snapshot['Status'] != "available":
            continue
        snapshot_age = evaluate_snapshot_age(manual_snapshot)
        delete_expired_snapshots(manual_snapshot, rds, snapshot_age)


def delete_expired_snapshots(manual_snapshot, rds, snapshot_age):
    """
    Helper function that deletes expired failsafe snapshots in accordance with
    the retention policy
    :param manual_snapshot: expired snapshots to be deleted
    :param rds: the Boto3 client used to interrogate AWS RDS services
    :param snapshot_age: evaluated age of the failsafe manual snapshot
    :return:
    """
    if snapshot_age.days >= SNAPSHOT_RETENTION_PERIOD_IN_DAYS:
        logger.warn("Deleting: {}"
                    .format(manual_snapshot['DBSnapshotIdentifier']))
        perform_delete(manual_snapshot['DBSnapshotIdentifier'], rds)
    else:
        logger.info("Not deleting snapshot - {} (it is only {} days old)"
                    .format(manual_snapshot['DBSnapshotIdentifier'],
                            snapshot_age.days))


def evaluate_snapshot_age(manual_snapshot):
    """
    Helper function to get age of snapshot as per current date
    :param manual_snapshot: manual snapshot in failsafe account
    :return: snapshot age
    """
    snapshot_date = manual_snapshot['SnapshotCreateTime']
    current_date = datetime.now(utc)
    snapshot_age = current_date - snapshot_date
    return snapshot_age


def get_snapshot_date(snapshot):
    """
    This is a helper function to ascertain snapshot has completed creating.
    When SnapshotCreateTime is present then the snapshot has finished creating
    :param snapshot: snapshot being created
    :return: datetime value of when snapshot was created
    """
    return datetime.now(utc) if snapshot['Status'] != 'available' \
        else snapshot['SnapshotCreateTime']


def get_snapshots(rds, **options):
    """
    This function performs an aws api call to get the snapshots depending on
    the arguments passed
    :param rds: the Boto3 client used to interrogate AWS RDS services
    :param options:
     db_instance_id: the specific instance to get snapshots from
     snapshot_type: can be 'manual' or 'shared' snapshot type
    :return: list of snapshots
    """
    instance = options.get('db_instance_id', '')
    snapshot_type = options.get('snapshot_type', '')
    return get_snapshots_by_filters(rds,
                                    db_instance_id=instance,
                                    snapshot_type=snapshot_type)


def get_snapshots_by_filters(rds, **options):
    snapshots = rds.describe_db_snapshots(
        SnapshotType=options.get('snapshot_type', ''),
        DBInstanceIdentifier=options.get('db_instance_id', ''),
        IncludeShared=True)['DBSnapshots']
    return sorted(snapshots, key=get_snapshot_date) if snapshots is not None \
        else None


def read_notification_payload(record, attribute):
    """
    Helper function to read the event payload passed through by sns
    :param record: snapshot object message
    :param attribute: the attribute being read and returned
    :return: returns instance or snapshot-id depending on attribute
    """
    message = json.loads(record['Sns']['Message'])
    return message[attribute]


def read_test_notification_payload(record, attribute):
    """
    Helper function to read the event payload passed through by test functions
    :param record: snapshot object message
    :param attribute: the attribute being read and returned
    :return: returns instance or snapshot-id depending on attribute
    """
    return json.loads(json.dumps(record['Sns']
                                       ['Message']))['default'][attribute]


def handler(event, context):
    """
    The function that AWS Lambda service invokes when executing the code.
    :param event: used to to pass in event data to the handler.
    The payload sent from the rdscopysnapshot function looks like:
    {
        'Instance': instance,
        'FailsafeSnapshotID': name_of_created_failsafe_snapshot
    }
    :param context: provides runtime information to the handler if required
    :return:
    """
    rds = client('rds', region_name=SERVICE_CONNECTION_DEFAULT_REGION)
    for record in event['Records']:
        if record['EventSource'] == 'aws:sns' and record['Sns']['Message']:
            if TESTING_HACK:
                instance = read_test_notification_payload(record, 'Instance')
                snapshot_id = read_test_notification_payload(record, 'FailsafeSnapshotID')
            else:
                instance = read_notification_payload(record, 'Instance')
                snapshot_id = read_notification_payload(record,
                                                        'FailsafeSnapshotID')
                logger.info('Retrieved Instance: {0} '
                            'and FailsafeSnapshotID: {1}'
                            .format(instance, snapshot_id))

        try:
            copy_manual_failsafe_snapshot_and_save(rds, instance, snapshot_id)
            delete_old_failsafe_manual_snapshots(rds, instance)
        except ClientError as e:
            logger.error(str(e))
    else:
        logger.info('No instances tagged for RDS failsafe backup found...')
