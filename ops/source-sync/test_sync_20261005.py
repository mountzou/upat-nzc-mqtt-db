"""Exercise source preservation and rollback with real Git and isolated files."""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

CONTROLLER = Path(__file__).with_name('sync_20261005.py')


class SyncIntegration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='source-sync-test-', dir=tempfile.gettempdir())
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'checkout'
        self.stage = Path(self.temp.name) / 'stage'
        self.root.mkdir(); self.stage.mkdir()
        spec = importlib.util.spec_from_file_location('source_sync', CONTROLLER)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.module.ROOT = self.root; self.module.STAGE = self.stage
        self.g('init', '-q', '-b', 'main')
        initial = {'.gitignore': '.env\n', 'README.md': 'upstream\n',
                   'docker-compose.prod.yml': 'services: {}\n', 'docker-compose.yml': 'dev\n',
                   'api/main.py': 'value = 1\n', 'db/init.sql': '-- old bootstrap\n',
                   'db/migrations/014_telemetry_timestamptz.sql': '-- old migration\n',
                   'obsolete.txt': 'retired source\n'}
        for name, text in initial.items(): self.write(name, text)
        self.g('add', '.'); self.commit('old'); self.old = self.g('rev-parse', 'HEAD').decode().strip()
        for name in ['api/main.py', 'db/init.sql', 'db/migrations/014_telemetry_timestamptz.sql']:
            self.write(name, 'value = 2\n' if name.endswith('.py') else '-- new compact source\n')
        self.write('docs/new.md', 'new source\n'); (self.root / 'obsolete.txt').unlink()
        self.g('add', '.'); self.commit('target'); self.target = self.g('rev-parse', 'HEAD').decode().strip()
        self.g('update-ref', 'refs/remotes/origin/main', self.target)
        self.g('reset', '--hard', self.old)
        (self.root / 'docker-compose.yml').unlink()
        os.symlink('docker-compose.prod.yml', self.root / 'docker-compose.yml')
        self.write('README.md', 'unrelated local notes\n')
        self.write('untracked-local.py', 'local = True\n')
        self.write('.env', 'LOCAL_TEST_SECRET=private\n'); (self.root / '.env').chmod(0o600)
        self.module.TARGET = self.target
        self.module.COMPOSE_SHA = self.module.sha((self.root / 'docker-compose.prod.yml').read_bytes())
        self.runtime_sha = self.module.sha(self.g('show', self.target + ':api/main.py'))
        self.module.runtime_sources = lambda: {'api/main.py': self.runtime_sha}
        self.module.inspect = lambda: {'/iot_api': {'status': 'running', 'id': 'unchanged'}}
        self.db_calls = 0
        def db():
            self.db_calls += 1
            return {'postgres_started_at': 'same', 'legacy_table': None, 'sequence_oid': 16445,
                    'compact_oid': 121068, 'latest': {'id': self.db_calls}}
        self.module.database = db
        self.health_calls = 0; self.fail_acceptance = False
        def health():
            self.health_calls += 1
            return {'status': 503} if self.fail_acceptance and self.health_calls > 1 else {
                'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
        self.module.health = health
        original_run = self.module.run
        self.module.run = lambda args, data=None: b'{"status":"PASS"}' if args[0] == 'python3' else original_run(args, data)
        files = self.stage / 'planned-files'; files.mkdir()
        actions = []
        for name in ['api/main.py', 'db/init.sql', 'db/migrations/014_telemetry_timestamptz.sql',
                     'docs/new.md', 'obsolete.txt']:
            here = self.module.state(name)
            wanted = None if name == 'obsolete.txt' else self.g('show', self.target + ':' + name)
            result_file = files / (self.module.sha(name.encode()) + '.result')
            if wanted is not None: result_file.write_bytes(wanted)
            actions.append({'path': name, 'operation': 'remove' if wanted is None else 'write',
                            'result_file': str(result_file) if wanted is not None else None,
                            'result_mode': '100644', 'current_sha256': here['sha256'] if here else None,
                            'result_sha256': self.module.sha(wanted) if wanted is not None else None})
        self.plan = {'target': self.target, 'old_head': self.old, 'actions': actions,
                     'runtime_differences_from_target': [], 'preserved_overrides': [{'path': 'README.md'}]}
        (self.stage / 'plan.json').write_text(json.dumps(self.plan))
        self.plan_sha = self.module.sha((self.stage / 'plan.json').read_bytes())
        self.original_argv = sys.argv
        self.addCleanup(setattr, sys, 'argv', self.original_argv)
        sys.argv = [str(CONTROLLER), '--apply', '--plan-sha256', self.plan_sha]

    def write(self, name, value):
        p = self.root / name; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(value)

    def g(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, stderr=subprocess.DEVNULL)

    def commit(self, message):
        self.g('-c', 'user.name=Sync Test', '-c', 'user.email=test@example.invalid',
               '-c', 'core.hooksPath=/dev/null', 'commit', '-qm', message)

    def assert_local_preserved(self):
        self.assertTrue((self.root / 'docker-compose.yml').is_symlink())
        self.assertEqual(os.readlink(self.root / 'docker-compose.yml'), 'docker-compose.prod.yml')
        self.assertEqual((self.root / '.env').stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.root / '.env').read_text(), 'LOCAL_TEST_SECRET=private\n')
        self.assertEqual((self.root / 'README.md').read_text(), 'unrelated local notes\n')
        self.assertEqual((self.root / 'untracked-local.py').read_text(), 'local = True\n')

    def test_success_preserves_overrides_and_updates_only_plan(self):
        self.module.main()
        self.assertEqual(self.g('rev-parse', 'HEAD').decode().strip(), self.target)
        self.assertFalse(self.g('diff', '--cached', '--name-only'))
        self.assertFalse((self.root / 'obsolete.txt').exists())
        self.assertEqual((self.root / 'docs/new.md').read_text(), 'new source\n')
        self.assert_local_preserved()
        self.assertEqual(json.loads((self.stage / 'accepted.json').read_text())['status'], 'PASS')

    def test_acceptance_failure_restores_git_and_files(self):
        self.fail_acceptance = True
        before_diff = self.g('diff', '--binary')
        with self.assertRaises(SystemExit): self.module.main()
        self.assertEqual(self.g('rev-parse', 'HEAD').decode().strip(), self.old)
        self.assertEqual(self.g('diff', '--binary'), before_diff)
        self.assertFalse(self.g('diff', '--cached', '--name-only'))
        self.assertTrue((self.root / 'obsolete.txt').is_file())
        self.assertFalse((self.root / 'docs').exists())
        self.assert_local_preserved()

    def test_changed_source_aborts_before_mutation(self):
        self.write('api/main.py', 'concurrent local edit\n')
        with self.assertRaises(AssertionError): self.module.main()
        self.assertEqual(self.g('rev-parse', 'HEAD').decode().strip(), self.old)
        self.assertFalse((self.stage / 'backup').exists())
        self.assert_local_preserved()


if __name__ == '__main__':
    unittest.main()
