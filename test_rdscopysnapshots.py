from datetime import tzinfo, timedelta, datetime

import pytest
import sure
from boto3 import client

import rdscopysnapshots as automated_snapshot_processor

REGION = 'ap-southeast-2'
INSTANCES = ["devarch-db"]

# Handle timezones correctly
ZERO = timedelta(0)


class UTC(tzinfo):
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()

rds = client("rds", region_name='ap-southeast-2', endpoint_url='http://localhost:5000')


def setup_module():
    instance_1 = rds.create_db_instance(DBInstanceIdentifier='devarch-db',
                                        AllocatedStorage=10,
                                        Engine='postgres',
                                        DBName='staging-postgres',
                                        DBInstanceClass='db.m1.small',
                                        LicenseModel='license-included',
                                        MasterUsername='root',
                                        MasterUserPassword='hunter2',
                                        Port=1234,
                                        DBSecurityGroups=["my_sg"])

    instance_2 = rds.create_db_instance(DBInstanceIdentifier='second_database',
                                        AllocatedStorage=10,
                                        Engine='mysql',
                                        DBName='staging-mysql',
                                        DBInstanceClass='db.m1.small',
                                        LicenseModel='license-included',
                                        MasterUsername='root',
                                        MasterUserPassword='hunter2',
                                        Port=1234,
                                        DBSecurityGroups=["my_sg"])

    instance_3 = rds.create_db_instance(DBInstanceIdentifier='third_database',
                                        AllocatedStorage=10,
                                        Engine='mysql',
                                        DBName='staging-mysql',
                                        DBInstanceClass='db.m1.small',
                                        LicenseModel='license-included',
                                        MasterUsername='root',
                                        MasterUserPassword='hunter2',
                                        Port=1234,
                                        DBSecurityGroups=["my_sg"])

    instance_4 = rds.create_db_instance(DBInstanceIdentifier='forth_database',
                                        AllocatedStorage=10,
                                        Engine='mysql',
                                        DBName='staging-mysql',
                                        DBInstanceClass='db.m1.small',
                                        LicenseModel='license-included',
                                        MasterUsername='root',
                                        MasterUserPassword='hunter2',
                                        Port=1234,
                                        Tags=[
                                            {
                                                'Key': 'Failsafe',
                                                'Value': 'false'
                                            },
                                        ],
                                        DBSecurityGroups=["my_sg"])

    failsafe_database_1 = rds.create_db_instance(DBInstanceIdentifier='failsafe_database_2',
                                                 AllocatedStorage=10,
                                                 Engine='mysql',
                                                 DBName='staging-mysql',
                                                 DBInstanceClass='db.m1.small',
                                                 LicenseModel='license-included',
                                                 MasterUsername='root',
                                                 MasterUserPassword='hunter2',
                                                 Port=1234,
                                                 Tags=[
                                                     {
                                                         'Key': 'Failsafe',
                                                         'Value': 'true'
                                                     },
                                                 ],
                                                 DBSecurityGroups=["my_sg"])

    failsafe_database_2 = rds.create_db_instance(DBInstanceIdentifier='failsafe_database',
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

    databases_created = [instance_1, instance_2, instance_3, instance_4, failsafe_database_1, failsafe_database_2]
    for db in databases_created:
        print db

    rds.create_db_snapshot(DBSnapshotIdentifier='failsafe-snapshot-1', DBInstanceIdentifier='failsafe_database')
    rds.create_db_snapshot(DBSnapshotIdentifier='snapshot-2', DBInstanceIdentifier='failsafe_database')
    rds.create_db_snapshot(DBSnapshotIdentifier='snapshot-3', DBInstanceIdentifier='failsafe_database')
    rds.create_db_snapshot(DBSnapshotIdentifier='snapshot-4', DBInstanceIdentifier='failsafe_database')
    rds.create_db_snapshot(DBSnapshotIdentifier='snapshot-5', DBInstanceIdentifier='failsafe_database')
    rds.create_db_snapshot(DBSnapshotIdentifier='snapshot-6', DBInstanceIdentifier='failsafe_database')


def test_rds_get_db_instances_with_failsafe_tag_true():
    db_instance_list = []
    instances = automated_snapshot_processor.get_db_instances_by_tags(rds, db_instance_list)
    print instances
    instances[0].should.be.equal('failsafe_database_2')
    instances[1].should.be.equal('failsafe_database')


def test_get_sorted_list_of_snapshots():
    sorted_snapshots_list = automated_snapshot_processor.get_snapshots(rds, 'failsafe_database', 'manual')
    for snapshot in sorted_snapshots_list:
        print snapshot['DBSnapshotIdentifier']
    sorted_snapshots_list[0]['DBSnapshotIdentifier'].should.be.equal('failsafe-snapshot-1')
    # TODO: find out why only one RDS Snapshot is returned. For delete snapshot all the snapshots are deleted


# TODO: Do not know how to test for automated snapshots
@pytest.mark.skip(reason="no way of currently testing this")
def test_create_failsafe_manual_snapshot():
    response = automated_snapshot_processor.create_failsafe_manual_snapshot(rds, 'failsafe_database')
    print response
    pass


def test_get_snapshot_date():
    snp = rds.describe_db_snapshots()['DBSnapshots'][0]
    # for s in snp['DBSnapshots']['DBSnapshotIdentifier']:
    # print snp
    resp = automated_snapshot_processor.get_snapshot_date(snp)
    print type(resp)
    isinstance(resp, datetime).should.be.true


def teardown_module():
    print "teardown_module"
    try:
        list_of_databases = rds.describe_db_instances()['DBInstances']
        for instance_payload in list_of_databases:
            db_instance = instance_payload['DBInstanceIdentifier']
            print("deleting database named ...{0}".format(db_instance))
            response = rds.delete_db_instance(
                DBInstanceIdentifier=db_instance,
                SkipFinalSnapshot=False)
        list_of_snapshots = rds.describe_db_snapshots()['DBSnapshots']
        for snapshots_payload in list_of_snapshots:
            snapshot_instance = snapshots_payload['DBSnapshotIdentifier']
            print("deleting snapshot named ...{0}".format(snapshot_instance))
            response = rds.delete_db_snapshot(DBSnapshotIdentifier=snapshot_instance)
        print response
    except Exception as error:
        print error

__all__ = ['sure']  # trick linting to consider python sure by exporting it
