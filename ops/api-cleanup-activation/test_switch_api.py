"""Pure checks for the production image-only configuration guard."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('switch_api',Path(__file__).with_name('switch_api.py'))
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class ImageOnlyTests(unittest.TestCase):
    def setUp(self):
        self.before={'services':{'api':{'image':'old','environment':{'TOKEN':'fixture'},'ports':['127.0.0.1:8000:8000']},
                                 'postgres':{'image':'postgres:15','volumes':['protected:/var/lib/postgresql/data']}},
                     'networks':{'default':{'name':'production'}}}
        self.after=copy.deepcopy(self.before)
        self.after['services']['api']['image']='new'

    def test_accepts_only_image(self):
        original=copy.deepcopy(self.before)
        module.image_only(self.before,self.after,'old','new')
        self.assertEqual(self.before,original)

    def test_rejects_secret_port_database_network_or_missing_service_changes(self):
        for edit in [lambda d:d['services']['api']['environment'].update(TOKEN='different'),
                     lambda d:d['services']['api'].update(ports=['8000:8000']),
                     lambda d:d['services']['postgres'].update(volumes=[]),
                     lambda d:d['networks']['default'].update(name='different'),
                     lambda d:d['services'].pop('postgres')]:
            with self.subTest(edit=edit):
                changed=copy.deepcopy(self.after);edit(changed)
                with self.assertRaises(RuntimeError):module.image_only(self.before,changed,'old','new')

    def test_rejects_baseline_drift_and_noop(self):
        for old,new in [('unexpected','new'),('old','old')]:
            with self.assertRaises(RuntimeError):module.image_only(self.before,self.after,old,new)

if __name__=='__main__':unittest.main()
