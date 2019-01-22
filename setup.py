import os
import shutil
import zipfile

from setuptools import setup, find_packages

from distutils.cmd import Command
try: # for pip >= 10
    from pip._internal.commands import WheelCommand
except ImportError: # for pip <= 9.0.3
    from pip.commands import WheelCommand

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements


PACKAGE_NAME = 'dataIngestionTool'
VERSION = '0.1'

with open("README.md", "r") as fh:
    long_description = fh.read()


reqs = parse_requirements('requirements.txt', session=False)
requirements = [str(ir.req) for ir in reqs]

test_reqs = parse_requirements('test_requirements.txt', session=False)
test_requirements = [str(ir.req) for ir in test_reqs]


class BdistSpark(Command):

    description = "create deps and project distribution files for spark_submit"
    user_options = [
        ('requirement=', 'r', 'Install from the given requirements file. [default: requirements.txt]'),
        ('wheel-dir=', 'w', 'Build deps into dir. [default: spark_dist]')
    ]

    def initialize_options(self):
        self.requirement = 'requirements.txt'
        self.wheel_dir = 'spark_dist'

    def finalize_options(self):
        assert os.path.exists(self.requirement), (
            "requirements file '{}' does not exist.".format(self.requirement))

    def run(self):
        if os.path.exists(self.wheel_dir):
            shutil.rmtree(self.wheel_dir)

        # generating deps wheels
        wheel_command = WheelCommand(isolated=False)
        wheel_command.main(args=['-r', self.requirement, '-w', self.wheel_dir])

        temp_dir = os.path.join(self.wheel_dir, '.temp')
        os.makedirs(temp_dir)

        z = zipfile.ZipFile(file=os.path.join(temp_dir, '{}-{}-deps.zip'.format(PACKAGE_NAME, VERSION)), mode='w')

        # making "fat" zip file with all deps from each wheel
        for dirname, _, files in os.walk(self.wheel_dir):
            self.rezip(z, dirname, files)
        z.close()

        cmd = self.reinitialize_command('bdist_wheel')
        cmd.dist_dir = temp_dir
        self.run_command('bdist_wheel')

        # make final rearrangements
        for dirname, _, files in os.walk(self.wheel_dir):
            for fname in files:
                if not fname.startswith(PACKAGE_NAME):
                    os.remove(os.path.join(self.wheel_dir, fname))
                else:
                    if fname.endswith('whl'):
                        os.renames(os.path.join(temp_dir, fname),
                                   os.path.join(self.wheel_dir, '{}-{}.zip'.format(PACKAGE_NAME, VERSION)))
                    else:
                        os.renames(os.path.join(temp_dir, fname), os.path.join(self.wheel_dir, fname))

    def rezip(self, z, dirname, files):
        if dirname == self.wheel_dir:
            for fname in files:
                full_fname = os.path.join(dirname, fname)
                w = zipfile.ZipFile(file=full_fname, mode='r')
                for file_info in w.filelist:
                    z.writestr(file_info, w.read(file_info.filename))


setup(
    name=PACKAGE_NAME,

    version=VERSION,

    description='Data Ingestion Tool',
    
    long_description=long_description,
    
    long_description_content_type="text/markdown",
    
    url="https://github.com/pypa/sampleproject",

    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],

    author='Sumit kumar Ankesh Jain',

    author_email='sumit.kumar@td.com;ankesh.jain@td.com',

    

    packages=find_packages(include=['dataIngestionTool', 'dataIngestionTool.*'],
                           exclude=['*.test.*', '*.test']),

    install_requires=requirements,

    tests_require=test_requirements,

    package_data={
        PACKAGE_NAME: ['../requirements.txt', '../test_requirements.txt']
    },

    cmdclass={
        "bdist_spark": BdistSpark
    }
)
