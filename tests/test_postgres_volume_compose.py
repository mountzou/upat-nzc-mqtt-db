"""Resolve real Compose models with synthetic credentials; never start services."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('check_compose',ROOT/'ops/postgres-volume/check-compose.py')
check=importlib.util.module_from_spec(spec);spec.loader.exec_module(check)


def model(file, root=ROOT, all_profiles=True):
    cmd=['docker','compose','--project-directory',str(root),'--env-file','/dev/null','-p','upat-nzc-mqtt-db']
    if all_profiles:cmd+=['--profile','*']
    cmd+=['-f',str(file),'config','--format','json']
    env={k:v for k,v in os.environ.items() if not k.startswith(('POSTGRES','COMPOSE','MQTT','TTN','SIMULATION','OPS_','AUTH_','DATA_'))}
    env.update(POSTGRES_USER='fixture',POSTGRES_PASSWORD='fixture-only',POSTGRES_DB='iot_db',POSTGRES_HOST='postgres',POSTGRES_PORT='15432',POSTGRES_INTERNAL_PORT='5432',MQTT_PORT='1883',OPS_TELEMETRY_TOKEN='fixture-only-token')
    return check.normalize(json.loads(subprocess.check_output(cmd,env=env,stderr=subprocess.PIPE,text=True)))


class VolumeComposeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.after=model(ROOT/'docker-compose.prod.yml')
        cls.tmp=tempfile.TemporaryDirectory();cls.addClassCleanup(cls.tmp.cleanup)

    def test_current_database_ownership(self):
        check.validate_model(self.after)
        self.assertNotIn('postgres', self.after['services'])
        self.assertNotIn('postgres_data', self.after.get('volumes', {}))
        self.assertEqual({'name': 'upat-nzc-mqtt-db_default', 'external': True},
                         self.after['networks']['default'])
        for service in ('shelly-ingestor', 'api'):
            environment = self.after['services'][service]['environment']
            self.assertFalse(any(key.startswith('SHELLY_MEASUREMENTS_') for key in environment))

    def test_jobs_profiles_preserved(self):
        actual=model(ROOT/'docker-compose.prod.yml',all_profiles=False)
        self.assertEqual({k:v for k,v in self.after['services'].items() if not v.get('profiles')},actual['services'])

    def test_default_symlink_resolves_identical_contract(self):
        root=Path(self.tmp.name)
        (root/'docker-compose.prod.yml').write_text((ROOT/'docker-compose.prod.yml').read_text())
        link=root/'docker-compose.yml'
        if not link.exists():link.symlink_to('docker-compose.prod.yml')
        self.assertEqual(model(root/'docker-compose.prod.yml'),model(link))

    def test_old_volume_and_pg_service_cannot_be_reintroduced(self):
        for change in ('service','volume','dependency','pgdata','network'):
            bad=copy.deepcopy(self.after)
            if change=='service':bad['services']['postgres']={}
            if change=='volume':bad.setdefault('volumes',{})['postgres_data']={}
            if change=='dependency':bad['services']['api']['depends_on']={'postgres':{}}
            if change=='pgdata':bad['services']['api']['volumes']=[{'target':'/var/lib/postgresql/data'}]
            if change=='network':bad['networks']['default']['external']=False
            with self.subTest(change=change),self.assertRaises(ValueError):check.validate_model(bad)

    def test_unrelated_image_or_environment_drift_rejected(self):
        for key,value in [('image','wrong-image'),('environment',{'POSTGRES_DB':'wrong-db'})]:
            bad=copy.deepcopy(self.after);bad['services']['api'][key]=value
            self.assertTrue(check.differences(self.after,bad))

    def test_local_development_keeps_its_database(self):
        development = model(ROOT/'docker-compose.yml')
        self.assertIn('postgres',development['services'])
        for service in ('shelly-ingestor','api'):
            self.assertFalse(any(key.startswith('SHELLY_MEASUREMENTS_') for key in development['services'][service]['environment']))


if __name__=='__main__':unittest.main()
