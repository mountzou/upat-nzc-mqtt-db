"""No Docker calls: reject unsafe target identities and storage consumers."""
import copy
import importlib.util
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('removal',ROOT/'ops/postgres-volume/remove-old-copy-20260916.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class RemovalGuards(unittest.TestCase):
    def setUp(self):
        self.old={'Id':m.OLD_ID,'Name':m.OLD_NAME,'State':{'Status':'exited','Running':False,'ExitCode':0},'HostConfig':{'RestartPolicy':{'Name':'no'}},'NetworkSettings':{'Networks':{}},'Mounts':[{'Name':m.OLD_VOLUME,'Source':m.OLD_PATH,'Destination':'/var/lib/postgresql/data','Type':'volume'}]}
        self.active={'Id':m.ACTIVE_ID,'Name':'/iot_postgres','State':{'Running':True},'Mounts':[{'Source':m.ACTIVE_PATH,'Destination':'/var/lib/postgresql/data','Type':'bind'}]}
        self.volume={'Name':m.OLD_VOLUME,'Mountpoint':m.OLD_PATH,'Driver':'local','Options':None}
    def test_exact_inactive_target_accepted(self):m.validate([self.old,self.active],self.volume)
    def test_running_old_target_rejected(self):
        self.old['State']['Running']=True
        with self.assertRaises(RuntimeError):m.validate([self.old,self.active],self.volume)
    def test_connected_old_target_rejected(self):
        self.old['NetworkSettings']['Networks']={'app':{}}
        with self.assertRaises(RuntimeError):m.validate([self.old,self.active],self.volume)
    def test_active_database_on_old_storage_rejected(self):
        self.active['Mounts'][0]['Source']=m.OLD_PATH
        with self.assertRaises(RuntimeError):m.validate([self.old,self.active],self.volume)
    def test_another_container_mounting_old_storage_or_parent_rejected(self):
        for path in [m.OLD_PATH,m.OLD_PATH+'/base','/var/lib/docker/volumes','/']:
            other={'Id':'other','Mounts':[{'Source':path}]}
            with self.subTest(path=path),self.assertRaises(RuntimeError):m.validate([self.old,self.active,other],self.volume)
    def test_volume_driver_bind_options_rejected(self):
        self.volume['Options']={'device':m.ACTIVE_PATH,'o':'bind'}
        with self.assertRaises(RuntimeError):m.validate([self.old,self.active],self.volume)
    def test_reused_container_name_with_different_id_rejected(self):
        self.old['Id']='replacement-id'
        with self.assertRaises(StopIteration):m.validate([self.old,self.active],self.volume)


if __name__=='__main__':unittest.main()
