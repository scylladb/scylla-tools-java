#!/usr/bin/python3

#
# Copyright (C) 2018 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import argparse
import io
import os
import tarfile
import pathlib


ap = argparse.ArgumentParser(description='Create a relocatable scylla package.')
ap.add_argument('--version', required=True,
                help='Tools version')
ap.add_argument('dest',
                help='Destination file (tar format)')

args = ap.parse_args()

version = args.version
output = args.dest

ar = tarfile.open(output, mode='w|gz')
pathlib.Path('build/SCYLLA-RELOCATABLE-FILE').touch()
ar.add('build/SCYLLA-RELOCATABLE-FILE', arcname='SCYLLA-RELOCATABLE-FILE')
ar.add('build/SCYLLA-RELEASE-FILE', arcname='SCYLLA-RELEASE-FILE')
ar.add('build/SCYLLA-VERSION-FILE', arcname='SCYLLA-VERSION-FILE')
ar.add('build/SCYLLA-PRODUCT-FILE', arcname='SCYLLA-PRODUCT-FILE')
ar.add('dist')
ar.add('conf')
ar.add('bin')
ar.add('tools')
ar.add('lib')
ar.add('doc')
ar.add('build/apache-cassandra-{}.jar'.format(version), arcname='apache-cassandra-{}.jar'.format(version))
ar.add('build/apache-cassandra-thrift-{}.jar'.format(version), arcname='apache-cassandra-thrift-{}.jar'.format(version))
ar.add('build/scylla-tools-{}.jar'.format(version), arcname='scylla-tools-{}.jar'.format(version))
ar.add('build/tools/lib/stress.jar', arcname='stress.jar')
ar.add('README.asc')
ar.add('CHANGES.txt')
ar.add('NEWS.txt')
ar.add('CASSANDRA-14092.txt')
ar.add('build/pylib', arcname='pylib')
