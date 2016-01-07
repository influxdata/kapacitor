#!/usr/bin/env python2.7

import sys
import os
import subprocess
import time
import datetime
import shutil
import tempfile
import hashlib
import re

try:
    import boto
    from boto.s3.key import Key
except ImportError:
    pass

# PACKAGING VARIABLES
INSTALL_ROOT_DIR = "/usr/bin"
LOG_DIR = "/var/log/kapacitor"
DATA_DIR = "/var/lib/kapacitor"
SCRIPT_DIR = "/usr/lib/kapacitor/scripts"

INIT_SCRIPT = "scripts/init.sh"
SYSTEMD_SCRIPT = "scripts/kapacitor.service"
POSTINST_SCRIPT = "scripts/post-install.sh"
POSTUNINST_SCRIPT = "scripts/post-uninstall.sh"
LOGROTATE_CONFIG = "etc/logrotate.d/kapacitor"
DEFAULT_CONFIG = "etc/kapacitor/kapacitor.conf"
PREINST_SCRIPT = None

# META-PACKAGE VARIABLES
PACKAGE_LICENSE = "MIT"
PACKAGE_URL = "github.com/influxdata/kapacitor"
MAINTAINER = "support@influxdata.com"
VENDOR = "InfluxData"
DESCRIPTION = "Time series data processing engine"

# SCRIPT START
prereqs = [ 'git', 'go' ]
optional_prereqs = [ 'fpm', 'rpmbuild' ]

fpm_common_args = "-f -s dir --log error \
 --vendor {} \
 --url {} \
 --after-install {} \
 --after-remove {} \
 --license {} \
 --maintainer {} \
 --config-files {} \
 --config-files {} \
 --directories {} \
 --description \"{}\"".format(
        VENDOR,
        PACKAGE_URL,
        POSTINST_SCRIPT,
        POSTUNINST_SCRIPT,
        PACKAGE_LICENSE,
        MAINTAINER,
        DEFAULT_CONFIG,
        LOGROTATE_CONFIG,
        ' --directories '.join([
                         LOG_DIR[1:],
                         DATA_DIR[1:],
                         SCRIPT_DIR[1:],
                         os.path.dirname(SCRIPT_DIR[1:]),
                         os.path.dirname(DEFAULT_CONFIG),
                    ]),
        DESCRIPTION)

targets = {
    'kapacitor' : './cmd/kapacitor/main.go',
    'kapacitord' : './cmd/kapacitord/main.go'
}


supported_arches = [ 'amd64' ]
supported_packages = {
    "darwin": [ "tar", "zip" ],
    "linux": [ "deb", "rpm", "tar", "zip" ],
    "windows": [ "zip" ],
}

def run(command, allow_failure=False, shell=False):
    out = None
    try:
        if shell:
            out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell)
        else:
            out = subprocess.check_output(command.split(), stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print ""
        print "Executed command failed!"
        print "-- Command run was: {}".format(command)
        print "-- Failure was: {}".format(e.output)
        if allow_failure:
            print "Continuing..."
            return out
        else:
            print ""
            print "Stopping."
            sys.exit(1)
    except OSError as e:
        print ""
        print "Invalid command!"
        print "-- Command run was: {}".format(command)
        print "-- Failure was: {}".format(e)
        if allow_failure:
            print "Continuing..."
            return out
        else:
            print ""
            print "Stopping."
            sys.exit(1)
    else:
        return out

def create_temp_dir():
    return tempfile.mkdtemp(prefix='kapacitor-build')

def get_current_version_tag():
    version = run("git describe --always --tags --abbrev=0").strip()
    return version

def get_current_version():
    version_tag = get_current_version_tag()
    # Remove leading 'v' and possible '-rc\d+'
    version = re.sub(r'-rc\d+', '', version_tag[1:])
    return version

def get_current_rc():
    rc = None
    version_tag = get_current_version_tag()
    matches = re.match(r'.*-rc(\d+)', version_tag)
    if matches:
        rc, = matches.groups(1)
    return rc

def get_current_commit(short=False):
    command = None
    if short:
        command = "git log --pretty=format:'%h' -n 1"
    else:
        command = "git rev-parse HEAD"
    out = run(command)
    return out.strip('\'\n\r ')

def get_current_branch():
    command = "git rev-parse --abbrev-ref HEAD"
    out = run(command)
    return out.strip()

def get_system_arch():
    return os.uname()[4]

def get_system_platform():
    if sys.platform.startswith("linux"):
        return "linux"
    else:
        return sys.platform

def check_path_for(b):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        full_path = os.path.join(path, b)
        if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
            return full_path

def check_environ(build_dir = None):
    print "\nChecking environment:"
    for v in [ "GOPATH", "GOBIN" ]:
        print "\t- {} -> {}".format(v, os.environ.get(v))

    cwd = os.getcwd()
    if build_dir == None and os.environ.get("GOPATH") not in cwd:
        print "\n!! WARNING: Your current directory is not under your GOPATH! This probably won't work."
    print ""

def check_prereqs():
    print "\nChecking for dependencies:"
    for req in prereqs:
        print "\t- {} ->".format(req),
        path = check_path_for(req)
        if path:
            print "{}".format(path)
        else:
            print "?"
    for req in optional_prereqs:
        print "\t- {} (optional) ->".format(req),
        path = check_path_for(req)
        if path:
            print "{}".format(path)
        else:
            print "?"
    print ""

def run_tests(race):
    get_command = "go get -d -t ./..."
    print "Retrieving Go dependencies...",
    run(get_command)
    print "done."
    print "Running tests..."
    test_command = "go test ./..."
    if race:
        test_command = "go test -race ./..."
    code = os.system(test_command)
    if code != 0:
        print "Tests Failed"
        return False
    else:
        print "Tests Passed"
        return True

def build(version=None,
          branch=None,
          commit=None,
          platform=None,
          arch=None,
          nightly=False,
          nightly_version=None,
          rc=None,
          race=False,
          clean=False,
          update=False,
          outdir='.'):
    print "Building for..."
    print "\t- version: {}".format(version)
    if rc:
        print "\t- release candidate: {}".format(rc)
    print "\t- commit: {}".format(commit)
    print "\t- branch: {}".format(branch)
    print "\t- platform: {}".format(platform)
    print "\t- arch: {}".format(arch)
    print "\t- nightly? {}".format(str(nightly).lower())
    print "\t- race enabled? {}".format(str(race).lower())
    print ""

    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir != '/':
        shutil.rmtree(outdir)
        os.makedirs(outdir)


    get_command = None
    if update:
        get_command = "go get -u -f -d ./..."
    else:
        get_command = "go get -d ./..."
    print "Retrieving Go dependencies...",
    run(get_command)
    print "done."

    print "Starting build..."
    for b, c in targets.iteritems():
        if platform == 'windows':
            b = b + '.exe'
        print "\t- Building '{}'...".format(b),
        build_command = ""
        build_command += "GOOS={} GOOARCH={} ".format(platform, arch)
        build_command += "go build -o {} ".format(os.path.join(outdir, b))
        if race:
            build_command += "-race "
        build_command += "-ldflags=\"-X main.buildTime='{}' ".format(datetime.datetime.utcnow().isoformat())
        build_command += "-X main.version={} ".format(version)
        build_command += "-X main.branch={} ".format(branch)
        build_command += "-X main.commit={}\" ".format(get_current_commit())
        build_command += c
        out = run(build_command, shell=True)
        print "[ DONE ]"
    print ""

def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        print e

def copy_file(fr, to):
    try:
        shutil.copy(fr, to)
    except OSError as e:
        print e

def create_package_fs(build_root):
    print "\t- Creating a filesystem hierarchy from directory: {}".format(build_root)
    # Using [1:] for the path names due to them being absolute
    # (will overwrite previous paths, per 'os.path.join' documentation)
    create_dir(os.path.join(build_root, INSTALL_ROOT_DIR[1:]))
    create_dir(os.path.join(build_root, LOG_DIR[1:]))
    create_dir(os.path.join(build_root, DATA_DIR[1:]))
    create_dir(os.path.join(build_root, SCRIPT_DIR[1:]))
    create_dir(os.path.join(build_root, os.path.dirname(DEFAULT_CONFIG)))
    create_dir(os.path.join(build_root, os.path.dirname(LOGROTATE_CONFIG)))

def package_scripts(build_root):
    print "\t- Copying scripts and configuration to build directory"
    shutil.copy(INIT_SCRIPT, os.path.join(build_root, SCRIPT_DIR[1:], INIT_SCRIPT.split('/')[1]))
    shutil.copy(SYSTEMD_SCRIPT, os.path.join(build_root, SCRIPT_DIR[1:], SYSTEMD_SCRIPT.split('/')[1]))
    shutil.copy(LOGROTATE_CONFIG, os.path.join(build_root, LOGROTATE_CONFIG))
    shutil.copy(DEFAULT_CONFIG, os.path.join(build_root, DEFAULT_CONFIG))

def generate_md5_from_file(path):
    m = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            m.update(chunk)
    return m.hexdigest()

def build_packages(build_output, version, rc):
    outfiles = []
    tmp_build_dir = create_temp_dir()
    try:
        print "Packaging..."
        for p in build_output:
            create_dir(os.path.join(tmp_build_dir, p))
            for a in build_output[p]:
                current_location = build_output[p][a]
                build_root = os.path.join(tmp_build_dir, p, a)
                create_dir(build_root)
                create_package_fs(build_root)
                package_scripts(build_root)
                for b in targets:
                    if p == 'windows':
                        b = b + '.exe'
                    fr = os.path.join(current_location, b)
                    to = os.path.join(build_root, INSTALL_ROOT_DIR[1:], b)
                    print "\t- [{}][{}] - Copying from '{}' to '{}'".format(p, a, fr, to)
                    copy_file(fr, to)
                for package_type in supported_packages[p]:
                    print "\t- Packaging directory '{}' as '{}'...".format(build_root, package_type),
                    name = 'kapacitor'
                    iteration = '1'
                    if rc is not None:
                        iteration = '0.rc{}'.format(rc)
                    if package_type in ['zip', 'tar']:
                        name = '{}-{}-{}_{}_{}'.format(name, version, iteration, p, a)
                    fpm_command = "fpm {} --name {} -t {} --version {} --iteration {} -C {} -p {} ".format(
                            fpm_common_args,
                            name,
                            package_type,
                            version,
                            iteration,
                            build_root,
                            current_location,
                        )
                    out = run(fpm_command, shell=True)
                    matches = re.search(':path=>"(.*)"', out)
                    outfile = None
                    if matches is not None:
                        outfile = matches.groups()[0]
                    if outfile is None:
                        print "Could not determine output file of fpm command"
                    else:
                        outfiles.append(outfile)
                    print "[ DONE ]"
                    # Display MD5 hash for generated package
                    print "\t\tMD5 = {}".format(generate_md5_from_file(outfile))
        print ""
        return outfiles
    finally:
        shutil.rmtree(tmp_build_dir)

def upload_packages(packages):
    print "Uploading packages to S3..."
    print ""

    c = boto.connect_s3()
    bucket = c.get_bucket('influxdb')
    for p in packages:
        name = os.path.basename(p)
        if bucket.get_key(name) is None:
            print "\t uploading {}...".format(name),
            k = Key(bucket)
            k.key = name
            n = k.set_contents_from_filename(p,replace=False)
            k.make_public()
            print "[ DONE ]"
        else:
            print "\t not uploading {} already exists".format(p)

    print ""


def main():
    print ""
    print "--- Kapacitor Builder ---"

    check_environ()

    outdir = 'build'
    commit = None
    target_platform = None
    target_arch = None
    nightly = False
    race = False
    nightly_version = None
    branch = None
    version = get_current_version()
    rc = get_current_rc()
    clean = False
    package = False
    update = False
    upload = False
    test = False

    for arg in sys.argv[1:]:
        if '--outdir' in arg:
            # Output build artifacts into dir.
            outdir = arg.split("=")[1]
        elif '--commit' in arg:
            # Commit to build from. If none is specified, then it will build from the most recent commit.
            commit = arg.split("=")[1]
        elif '--branch' in arg:
            # Branch to build from. If none is specified, then it will build from the current branch.
            branch = arg.split("=")[1]
        elif '--arch' in arg:
            # Target architecture. If none is specified, then it will build for the current arch.
            target_arch = arg.split("=")[1]
        elif '--platform' in arg:
            # Target platform. If none is specified, then it will build for the current platform.
            target_platform = arg.split("=")[1]
        elif '--rc' in arg:
            # Signifies that this is a release candidate build.
            rc = arg.split("=")[1]
        elif '--race' in arg:
            # Signifies that race detection should be enabled.
            race = True
        elif '--clean' in arg:
            # Signifies that the outdir should be deleted before building
            clean = True
        elif '--package' in arg:
            # Signifies that distribution packages will be built
            package = True
        elif '--nightly' in arg:
            # Signifies that this is a nightly build.
            nightly = True
            # In order to support nightly builds on the repository, we are adding the epoch timestamp
            # to the version so that seamless upgrades are possible.
            version = "{}.n{}".format(version, int(time.time()))
        elif '--update' in arg:
            # Signifies that deps should be checked for updates
            update = True
        elif '--upload' in arg:
            # Signifies that the resulting packages should be uploaded to S3
            upload = True
        elif '--test' in arg:
            # Run tests and exit
            test = True
        else:
            print "!! Unknown argument: {}".format(arg)
            sys.exit(1)

    if nightly and rc:
        print "!! Cannot be both nightly and a release candidate! Stopping."
        sys.exit(1)

    if not commit:
        commit = get_current_commit(short=True)
    if not branch:
        branch = get_current_branch()
    if not target_arch:
        target_arch = get_system_arch()
    if not target_platform:
        target_platform = get_system_platform()

    if target_arch == "x86_64":
        target_arch = "amd64"

    # TODO(rossmcdonald): Prepare git repo for build (checking out correct branch/commit, etc.)
    # prepare(branch=branch, commit=commit)

    if test:
        if not run_tests(race):
            return 1
        return 0



    check_prereqs()

    single_build = True
    build_output = {}
    platforms = []
    arches = []
    if target_platform == 'all':
        platforms = supported_packages.keys()
        single_build = False
    else:
        platforms = [target_platform]

    if target_arch == 'all':
        arches = supported_arches
        single_build = False
    else:
        arches = [target_arch]

    # Run builds
    for platform in platforms:
        for arch in arches:
            od = outdir
            if not single_build:
                od = os.path.join(outdir, platform, arch)
            build(version=version,
                  branch=branch,
                  commit=commit,
                  platform=platform,
                  arch=arch,
                  nightly=nightly,
                  nightly_version=nightly_version,
                  rc=rc,
                  race=race,
                  clean=clean,
                  update=update,
                  outdir=od)
            build_output.update( { platform : { arch : od } } )

    # Build packages
    if package:
        if not check_path_for("fpm"):
            print "!! Cannot package without command 'fpm'. Stopping."
            sys.exit(1)
        packages = build_packages(build_output, version, rc)
        if upload:
            upload_packages(packages)
    return 0

if __name__ == '__main__':
    exit(main())
