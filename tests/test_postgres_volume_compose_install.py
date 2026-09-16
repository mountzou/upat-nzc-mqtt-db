"""Exercise config-only install and failure rollback on a temporary filesystem."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('installer',ROOT/'ops/postgres-volume/install-compose.py')
i=importlib.util.module_from_spec(spec);spec.loader.exec_module(i)


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'host';self.root.mkdir()
        self.stage=Path(self.tmp.name)/'stage';self.stage.mkdir()
        self.old={'docker-compose.prod.yml':b'old production\n','docker-compose.yml':b'old development\n'}
        for rel,data in self.old.items():(self.root/rel).write_bytes(data)
        (self.root/'.env').write_text('SECRET=private-fixture\n')
        (self.root/'untouched').write_text('must remain')
        files={}
        for rel in i.FILES:
            p=self.stage/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('candidate '+rel)
            files[rel]=i.sha(p)
        self.manifest={'commit':'fixture-commit','files':files,'expected_before':{rel:i.sha(self.root/rel) for rel in self.old}}
        (self.stage/'ARTIFACT-MANIFEST.json').write_text(json.dumps(self.manifest))
        self.checker=Mock();self.checker.verify.return_value={'status':'PASS'}
        self.checker.stable_state.return_value={'postgres':'same-running-container'}

    def install(self):
        stream=io.StringIO()
        with patch.object(i,'ROOT',self.root),patch.object(i.os,'geteuid',return_value=0),patch.object(i,'load_checker',return_value=self.checker),contextlib.redirect_stdout(stream):
            i.main(self.stage)
        self.assertNotIn('private-fixture',stream.getvalue())
        return json.loads(stream.getvalue())

    def old_files_intact(self):
        for rel,data in self.old.items():
            self.assertFalse((self.root/rel).is_symlink())
            self.assertEqual(data,(self.root/rel).read_bytes())
        self.assertEqual('must remain',(self.root/'untouched').read_text())

    def test_install_only_configs_and_canonical_symlink(self):
        result=self.install()
        self.assertEqual('PASS',result['status'])
        self.assertFalse(result['lifecycle_commands_executed'])
        self.assertEqual('docker-compose.prod.yml',str((self.root/'docker-compose.yml').readlink()))
        self.assertEqual((self.root/'docker-compose.yml').read_bytes(),(self.stage/'docker-compose.prod.yml').read_bytes())
        for rel,data in self.old.items():self.assertEqual(data,(self.stage/'before'/rel).read_bytes())
        self.assertEqual('must remain',(self.root/'untouched').read_text())

    def test_modified_artifact_rejected_before_install(self):
        (self.stage/'docker-compose.prod.yml').write_text('tampered')
        with self.assertRaisesRegex(RuntimeError,'artifact checksum'):self.install()
        self.old_files_intact()

    def test_source_drift_rejected(self):
        (self.root/'docker-compose.prod.yml').write_text('operator edit')
        with self.assertRaisesRegex(RuntimeError,'source changed'):self.install()
        self.assertEqual('operator edit',(self.root/'docker-compose.prod.yml').read_text())
        self.assertFalse((self.stage/'before').exists())

    def test_postcheck_failure_restores_both_entrypoints(self):
        self.checker.verify.side_effect=[{'status':'PASS'},ValueError('failed')]
        with self.assertRaises(ValueError):self.install()
        self.old_files_intact()
        self.assertTrue((self.stage/'CONFIG-ROLLBACK.json').exists())
        self.assertFalse((self.root/'ops/postgres-volume/check-compose.py').exists())

    def test_container_change_restores_config(self):
        self.checker.stable_state.side_effect=[{'db':'before'},{'db':'changed'}]
        with self.assertRaisesRegex(RuntimeError,'container state changed'):self.install()
        self.old_files_intact()

    def test_edit_during_preflight_rejected(self):
        def change(*args):
            (self.root/'docker-compose.yml').write_text('operator edit')
            return {'status':'PASS'}
        self.checker.verify.side_effect=change
        with self.assertRaisesRegex(RuntimeError,'source changed during preflight'):self.install()
        self.assertEqual('operator edit',(self.root/'docker-compose.yml').read_text())
        self.assertEqual(self.old['docker-compose.prod.yml'],(self.root/'docker-compose.prod.yml').read_bytes())


if __name__=='__main__':unittest.main()
