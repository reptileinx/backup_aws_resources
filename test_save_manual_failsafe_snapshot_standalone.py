import datetime
import sure
from boto3 import client
from mock import MagicMock
from moto import mock_rds2

import rdssavesnapshot as save_service


@mock_rds2
def test_delete_snapshot_before_copying():
    rds = client('rds', region_name='ap-southeast-2')
    save_service.logger = MagicMock()
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_5',
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

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-5', DBInstanceIdentifier='failsafe_database_5')
    manual_snapshots = save_service.get_snapshots(rds, db_instance_id='failsafe_database_5', snapshot_type='manual')
    save_service.local_snapshot_deletion_required('failsafe-snapshot-5', manual_snapshots)
    save_service.logger. \
        warn.assert_called_with('Local copy of failsafe-snapshot-5 already exists - deleting it before copying')

@mock_rds2
def test_local_snapshot_deletion_required():
    rds = client('rds', region_name='ap-southeast-2')
    save_service.logger = MagicMock()
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_5',
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

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-5', DBInstanceIdentifier='failsafe_database_5')
    manual_snapshots = save_service.get_snapshots(rds, db_instance_id='failsafe_database_5', snapshot_type='manual')
    save_service.local_snapshot_deletion_required('failsafe-snapshot-52313', manual_snapshots).should_not.be.true



@mock_rds2
def test_evaluate_snapshot_age():
    rds = client('rds', region_name='ap-southeast-2')
    save_service.logger = MagicMock()
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_5',
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

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-5', DBInstanceIdentifier='failsafe_database_5')
    manual_snapshot = save_service.get_snapshots(rds, db_instance_id='failsafe_database_5', snapshot_type='manual')[0]
    age = save_service.evaluate_snapshot_age(manual_snapshot)
    age.days.should.be(-1)


@mock_rds2
def test_delete_expired_snapshots():
    rds = client('rds', region_name='ap-southeast-2')
    save_service.logger = MagicMock()
    rds.create_db_instance(DBInstanceIdentifier='failsafe_database_5',
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

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-5', DBInstanceIdentifier='failsafe_database_5')
    manual_snapshot = save_service.get_snapshots(rds, db_instance_id='failsafe_database_5', snapshot_type='manual')[0]
    save_service.perform_delete = MagicMock()
    save_service.delete_expired_snapshots(manual_snapshot, rds, datetime.timedelta(50))
    save_service.perform_delete.assert_called()


@mock_rds2
def test_handler_prints_message_when_no_instance_with_tags_is_found():
    event = MagicMock()
    save_service.logger = MagicMock()
    save_service.handler(event, None)
    save_service.logger.info.assert_called_with('No instances tagged for RDS failsafe backup have been found...')


@mock_rds2
def test_save_logs_warning_when_no_snapshot_is_shared():
    save_service.TESTING_HACK = True
    event = setup_event()
    save_service.get_snapshots = MagicMock(return_value=None)
    save_service.delete_old_failsafe_manual_snapshots = MagicMock()
    save_service.logger = MagicMock()
    save_service.handler.when.called_with(event, None).should.have.raised(save_service.ClientException)
    save_service.logger.warn.assert_called_with('No shared snapshots found.')


@mock_rds2
def test_error_is_logged_if_shared_snapshot_is_not_found():
    rds = client('rds', region_name='ap-southeast-2')
    instance = 'some_db'
    failsafe_snapshot_id = 'some_snapshot_id'
    save_service.get_snapshots = MagicMock()
    save_service.logger = MagicMock()
    save_service.copy_manual_failsafe_snapshot_and_save(rds, instance, failsafe_snapshot_id)
    save_service.logger.error.assert_called_with(
        'Shared snapshot with id ...:snapshot:{} failed to copy.'.format(failsafe_snapshot_id))


@mock_rds2
def test_copy_manual_failsafe_snapshot_and_save_no_shared_snapshot():
    rds = client('rds', region_name='ap-southeast-2')
    save_service.logger = MagicMock()
    save_service.get_snapshots = MagicMock(return_value=None)
    save_service.copy_manual_failsafe_snapshot_and_save \
        .when.called_with(rds, 'instance', 'snapshot-1').should.have.raised(save_service.ClientException)
    save_service.logger.warn.assert_called_with('No shared snapshots found.')


def get_response():
    return [
        {u'Engine': 'mysql', u'SnapshotCreateTime': (2017, 10, 6, 4, 0, 27, 727000,),
         u'AvailabilityZone': 'ap-southeast-2a',
         u'PercentProgress': 100, u'MasterUsername': 'admin', u'Encrypted': False,
         u'LicenseModel': 'general-public-license', u'StorageType': 'gp2', u'Status': 'available',
         u'VpcId': 'vpc-g4463543', u'DBSnapshotIdentifier': 'db-under-test-snaps',
         u'InstanceCreateTime': (2017, 10, 5, 4, 45, 30, 702000,),
         u'OptionGroupName': 'default:mysql-5-6', u'AllocatedStorage': 50, u'EngineVersion': '5.6.27',
         u'SnapshotType': 'manual', u'IAMDatabaseAuthenticationEnabled': False, u'Port': 3306,
         u'DBInstanceIdentifier': 'db-under-test'},
        {u'MasterUsername': 'admin', u'LicenseModel': 'general-public-license',
         u'InstanceCreateTime': (2017, 10, 5, 4, 45, 30, 702000,), u'Engine': 'mysql',
         u'VpcId': 'vpc-g4463543', u'SourceRegion': 'ap-southeast-2', u'AllocatedStorage': 50, u'Status': 'available',
         u'PercentProgress': 100,
         u'DBSnapshotIdentifier': 'failsafe-db-under-test-snap',
         u'EngineVersion': '5.6.27', u'OptionGroupName': 'default:mysql-5-6',
         u'SnapshotCreateTime': (2017, 10, 8, 5, 14, 1, 181000,),
         u'AvailabilityZone': 'ap-southeast-2a', u'StorageType': 'gp2', u'Encrypted': False,
         u'IAMDatabaseAuthenticationEnabled': False, u'SnapshotType': 'manual', u'Port': 3306,
         u'DBInstanceIdentifier': 'db-under-test'}]


def get_snapshots(*args, **kwargs):
    if kwargs['db_instance_id'] is '':
        return [
            {
                'DBSnapshotIdentifier': 'failsafe-db-under-test-snap',
                'DBInstanceIdentifier': 'db-under-test'
            }
        ]
    return get_response()


@mock_rds2
def test_end_to_end_method_called_in_correct_order():
    rds = client('rds', region_name='ap-southeast-2')
    event = setup_event()
    save_service.TESTING_HACK = True
    save_service.delete_old_failsafe_manual_snapshots = MagicMock()
    m = MagicMock()
    save_service.get_snapshots = MagicMock(side_effect=get_snapshots)
    shared_snapshots = m.get_snapshots(rds, db_instance_id='', snapshot_type='shared')
    manual_snapshots = m.get_snapshots(rds, db_instance_id='failsafe_database_1', snapshot_type='shared')
    save_service.re.search = MagicMock()
    save_service.match_shared_snapshot_requiring_copy = MagicMock(return_value=True)
    save_service.delete_duplicate_snapshots = MagicMock()
    save_service.read_test_notification_payload = MagicMock()
    save_service.copy_failsafe_snapshot = MagicMock()
    save_service.read_notification_payload = MagicMock()
    save_service.handler(event, None)
    save_service.read_test_notification_payload.assert_called()
    save_service.read_notification_payload.assert_not_called()
    save_service.delete_duplicate_snapshots.assert_called_once()
    save_service.copy_failsafe_snapshot.assert_called()


@mock_rds2
def test_get_snapshots_with_no_instance_name():
    rds = client('rds', region_name='ap-southeast-2')
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

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-1', DBInstanceIdentifier='failsafe_database_1')

    instance = ''
    snapshot_type = 'manual'
    list_of_snapshots = save_service.get_snapshots(rds, db_instance_id=instance, snapshot_type=snapshot_type)
    type(list_of_snapshots).should.be(list)
    list_of_snapshots.should.have.length_of(1)


@mock_rds2
def test_get_snapshots_with_instance_name():
    rds = client('rds', region_name='ap-southeast-2')
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

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-2', DBInstanceIdentifier='failsafe_database_2')

    instance = 'failsafe_database_2'
    snapshot_type = 'manual'
    list_of_snapshots = save_service.get_snapshots(rds, db_instance_id=instance, snapshot_type=snapshot_type)
    type(list_of_snapshots).should.be(list)
    list_of_snapshots[0]['DBSnapshotIdentifier'].should_not.be.empty


def setup_event():
    return {
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:EXAMPLE",
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "1970-01-01T00:00:00.000Z",
                    "Signature": "EXAMPLE",
                    "SigningCertUrl": "EXAMPLE",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "Message": {
                        "default": {
                            "Instance": "db-under-test",
                            "FailsafeSnapshotID": "failsafe-db-under-test-snap"}
                    },
                    "MessageAttributes": {
                        "Test": {
                            "Type": "String",
                            "Value": "TestString"
                        },
                        "TestBinary": {
                            "Type": "Binary",
                            "Value": "TestBinary"
                        }
                    },
                    "Type": "Notification",
                    "UnsubscribeUrl": "EXAMPLE",
                    "TopicArn": "arn:aws:sns:EXAMPLE",
                    "Subject": "TestInvoke"
                }
            }
        ]
    }


__all__ = ['sure']  # trick linting to consider python sure by exporting it
