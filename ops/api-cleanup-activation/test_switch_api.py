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


class ArchiveTests(unittest.TestCase):
    def archive(self, directory, damage=None):
        import hashlib, io, json, tarfile
        blobs={}
        def blob(value, media):
            data=json.dumps(value).encode() if isinstance(value,dict) else value
            digest='sha256:'+hashlib.sha256(data).hexdigest()
            blobs['blobs/sha256/'+digest.split(':')[1]]=data
            return dict(mediaType=media,digest=digest,size=len(data))
        config=blob({'architecture':'amd64'},'application/vnd.oci.image.config.v1+json')
        layer=blob(b'fixture layer','application/vnd.oci.image.layer.v1.tar')
        manifest=blob({'config':config,'layers':[layer]},'application/vnd.oci.image.manifest.v1+json')
        root=blob({'manifests':[manifest]},'application/vnd.oci.image.index.v1+json')
        blobs['index.json']=json.dumps({'manifests':[root]}).encode()
        key='blobs/sha256/'+layer['digest'].split(':')[1]
        if damage=='corrupt':blobs[key]=b'x'*len(blobs[key])
        if damage=='missing':del blobs[key]
        path=Path(directory)/'image.tar'
        with tarfile.open(path,'w') as tf:
            for name,data in blobs.items():
                entry=tarfile.TarInfo(name);entry.size=len(data);tf.addfile(entry,io.BytesIO(data))
        return path,root['digest']

    def test_validates_index_manifest_config_and_layer(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path,identity=self.archive(d)
            self.assertEqual(module.verify_archive(path,identity),4)

    def test_rejects_tampered_or_missing_layers(self):
        import tempfile
        for damage in ['corrupt','missing']:
            with self.subTest(damage=damage),tempfile.TemporaryDirectory() as d:
                path,identity=self.archive(d,damage)
                with self.assertRaises((RuntimeError,KeyError)):module.verify_archive(path,identity)

    def test_rejects_wrong_image(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path,identity=self.archive(d)
            with self.assertRaises(RuntimeError):module.verify_archive(path,'sha256:'+'0'*64)

if __name__=='__main__':unittest.main()

