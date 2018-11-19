import sure
from boto3 import client
from botocore.exceptions import ClientError
from mock import MagicMock
from moto import mock_rds2, mock_sns
import os

os.environ['FAILSAFE_ACCOUNT_ID'] = '23423525334242'

import rdscopysnapshots as copy_service


def test_get_snapshot_date_is_now_when_snapshot_is_not_available():
    snapshot = MagicMock()
    copy_service.datetime = MagicMock(return_value='2017, 10, 13, 13, 37, 33, 521429')
    copy_service.get_snapshot_date(snapshot).should.equal(copy_service.datetime.now())


@mock_rds2
def test_get_snapshot_date_is_snapshot_creation_time_when_snapshot_is_available():
    rds = client("rds", region_name="ap-southeast-2")
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_1',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])
    resp = rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-1',
                                  DBInstanceIdentifier='failsafe_database_1')
    copy_service.get_snapshot_date(resp['DBSnapshot']).should.equal(resp['DBSnapshot']['SnapshotCreateTime'])


@mock_rds2
def test_copy_manual_failsafe_snapshot_and_save():
    rds = client("rds", region_name="ap-southeast-2")
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_1',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])

    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_2',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'false'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_3',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'NotFailsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-1', DBInstanceIdentifier='failsafe_database_1')
    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-2', DBInstanceIdentifier='failsafe_database_2')

    response = copy_service.get_snapshots(rds, 'failsafe_database_1', 'Manual')
    response[0]["DBSnapshotIdentifier"].should.equal('failsafe-snapshot-1')


@mock_rds2
def test_create_failsafe_manual_snapshot_exists_if_snapshot_exists():
    rds = client("rds", region_name="ap-southeast-2")

    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_1',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])

    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_2',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'false'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_2',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'NotFailsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-1', DBInstanceIdentifier='failsafe_database_1')
    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-2', DBInstanceIdentifier='failsafe_database_2')

    rds.modify_db_snapshot_attribute = MagicMock()

    copy_service.create_name_of_failsafe_snapshot = MagicMock(return_value='failsafe-snapshot-1')
    copy_service.wait_until_failsafe_snapshot_is_available = MagicMock(return_value={'available=False'})
    copy_service.get_name_of_newest_automated_snapshot = MagicMock(return_value='rds:snapshot-1')
    rds.copy_db_snapshot = MagicMock(
        return_value={'DBSnapshot': {'Timezone': 'string',
                                     'DBSnapshotIdentifier': copy_service.create_name_of_failsafe_snapshot(
                                         'rds:snapshot-1', 'failsafe-'),
                                     'IAMDatabaseAuthenticationEnabled': True,
                                     'DBSnapshotArn': 'string',
                                     'DBInstanceIdentifier': 'failsafe_database_1'}})

    copy_service.logger = MagicMock()
    copy_service.create_failsafe_manual_snapshot.when.called_with(rds, 'failsafe_database_1').should.have.return_value(
        'failsafe-snapshot-1')
    copy_service.logger.warn.assert_called_with(
        'Manual snapshot already exists for the automated snapshot rds:snapshot-1')


@mock_rds2
def test_create_failsafe_manual_snapshot_copies_automated_snapshot():
    name_of_rds_automated_snapshot = 'rds:snapshot-3'

    rds = client("rds", region_name="ap-southeast-2")
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_1',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])

    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_2',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'false'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_3',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'NotFailsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-1', DBInstanceIdentifier='failsafe_database_1')
    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-2', DBInstanceIdentifier='failsafe_database_2')

    copy_service.logger = MagicMock()
    copy_service.create_name_of_failsafe_snapshot = MagicMock(return_value='failsafe-snapshot-3')
    copy_service.wait_until_failsafe_snapshot_is_available = MagicMock(return_value={'available=True'})
    copy_service.get_name_of_newest_automated_snapshot = MagicMock(return_value=name_of_rds_automated_snapshot)
    rds.copy_db_snapshot = MagicMock(
        return_value={'DBSnapshot': {'Timezone': 'string',
                                     'DBSnapshotIdentifier': copy_service.create_name_of_failsafe_snapshot(
                                         name_of_rds_automated_snapshot),
                                     'IAMDatabaseAuthenticationEnabled': True,
                                     'DBSnapshotArn': 'string',
                                     'DBInstanceIdentifier': 'failsafe_database_1'}})

    response = copy_service.create_failsafe_manual_snapshot(rds, 'failsafe_database_1')
    response.should.be.true
    copy_service.logger.info.assert_called_with('Snapshot rds:snapshot-3 copied to failsafe-snapshot-3')


@mock_rds2
def test_get_db_from_event():
    response = copy_service.get_db_instances_from_notification(get_event())
    response.should.contain('reptileinx-02-db')


@mock_rds2
def test_unrecoverable_exception_is_raised_when_delete_snapshot_fails_with_no_snapshot_to_delete():
    copy_service.logger = MagicMock()
    copy_service.handler.when.called_with(get_event(), None).should_not.throw(ClientError)
    copy_service.logger.info.assert_called_with(
        "Preparing deletion of previously created manual snapshots for DB instance - reptileinx-02-db")


@mock_rds2
def test_unrecoverable_exception_is_ignored_and_delete_continues():
    rds = client("rds", region_name="ap-southeast-2")
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_1',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_2',
                           AllocatedStorage=10,
                           Engine='postgres',
                           DBName='staging-postgres',
                           DBInstanceClass='db.m1.small',
                           LicenseModel='license-included',
                           MasterUsername='root_failsafe',
                           MasterUserPassword='hunter_failsafe',
                           Port=3000,
                           Tags=[
                               {
                                   'Key': 'Failsafe',
                                   'Value': 'true'
                               },
                           ],
                           DBSecurityGroups=["my_sg"])
    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-2',
                           DBInstanceIdentifier='failsafe_database_2')
    copy_service.logger = MagicMock()
    copy_service.share_failsafe_snapshot = MagicMock()
    copy_service.send_sns_to_failsafe_account = MagicMock()
    copy_service.create_failsafe_manual_snapshot = MagicMock(return_value=True)
    copy_service.handler.when.called_with(None, None).should.throw(TypeError)


@mock_sns
def test_get_subscription_sns_topic_arn_throws_error_when_sns_arn_not_found():
    copy_service.logger = MagicMock()
    sns = client('sns', region_name='ap-southeast-2')
    sns.create_topic(Name='TopicName')
    copy_service.get_subscription_sns_topic_arn()
    copy_service.logger.error.assert_called_with(
        'Initial setup required. Failsafe SNS topic reptileinx_save_failsafe_snapshot_sns_topic not found.')


@mock_sns
def test_send_sns_to_failsafe_account():
    sns = client('sns', region_name='ap-southeast-2')
    topic_arn = 'arn:aws:sns:ap-southeast-2:280000000083:SnsRdsSave'
    sns.publish = MagicMock()
    copy_service.get_subscription_sns_topic_arn = MagicMock(return_value=topic_arn)
    copy_service.send_sns_to_failsafe_account = MagicMock()
    copy_service.send_sns_to_failsafe_account. \
        when.called_with('failsafe_database',
                         'failsafe-snapshot').should_not.throw(ClientError)
    copy_service.send_sns_to_failsafe_account \
        .when.called_with('failsafe_database',
                          'failsafe-snapshot').should_not.throw(copy_service.ClientException)


def create_name_of_failsafe_snapshot_returns_name_with_prefix():
    copy_service.create_name_of_failsafe_snapshot = MagicMock()
    copy_service.create_failsafe_manual_snapshot \
        .when.called_with('rds:i_am_an_automated_snapshot',
                          'failsafe-').should.have.return_value('failsafe-i_am_an_automated_snapshot')


def test_event_guard():
    copy_service.logger = MagicMock()
    copy_service.event_guard(get_event())
    copy_service.logger.info.assert_called_with('received event RDS-EVENT-0002 from RDS')


def get_event():
    event = {
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:ap-southeast-2:129000003686:reptileinx_snapshot:183d5f808",
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "2017-11-26T16:09:22.468Z",
                    "Signature": "HK7LOsTVTwrNlObnsDUwp1/VuQWqPPdDJ/+knKv9OdfcouZwsx3GopiQ==",
                    "SigningCertUrl": "https://sns.ap-southeast-2.amazonaws.com/SimpleNotificationService-4330041.pem",
                    "MessageId": "545cb72c-c453-5b99-9219-e5b82d2152a4",
                    "Message": {
                        "Event Source": "db-instance",
                        "Event Time": "2017-11-26 16:05:27.306",
                        "Identifier Link": "https://console.aws.amazon.com/rds/home?reg-02-db",
                        "Source ID": "reptileinx-02-db",
                        "Event ID": "http://docs.amzonw.com/AmazRDS/latest/UserGuide/USER_Events.html#RDS-EVENT-0002",
                        "Event Message": "Backing up DB instance"
                    },
                    "MessageAttributes": {},
                    "Type": "Notification",
                    "TopicArn": "arn:aws:sns:ap-southeast-2:129000003686:reptileinx_copy_failsafe_snapshot_sns_topic",
                    "Subject": "RDS Notification Message"
                }
            }
        ]
    }
    return event


__all__ = ['sure']  # trick linting to consider python sure by exporting it
