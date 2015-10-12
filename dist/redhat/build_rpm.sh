#!/bin/sh -e

RPMBUILD=`pwd`/build/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla-tools-java dir"
    exit 1
fi

sudo yum install -y rpm-build git

OS=`awk '{print $1}' /etc/redhat-release`
if [ "$OS" = "Fedora" ] && [ ! -f /usr/bin/mock ]; then
    sudo yum -y install mock
elif [ "$OS" = "CentOS" ] && [ ! -f /usr/bin/yum-builddep ]; then
    sudo yum -y install yum-utils
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
git archive --format=tar --prefix=scylla-tools-$SCYLLA_VERSION/ HEAD -o build/rpmbuild/SOURCES/scylla-tools-$VERSION.tar
cp dist/redhat/scylla-tools.spec.in $RPMBUILD/SPECS/scylla-tools.spec
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" $RPMBUILD/SPECS/scylla-tools.spec
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" $RPMBUILD/SPECS/scylla-tools.spec

if [ "$OS" = "Fedora" ]; then
    rpmbuild -bs --define "_topdir $RPMBUILD" $RPMBUILD/SPECS/scylla-tools.spec
    /usr/bin/mock rebuild --resultdir=`pwd`/build/rpms $RPMBUILD/SRPMS/scylla-tools-$VERSION*.src.rpm
else
    sudo yum-builddep -y  $RPMBUILD/SPECS/scylla-tools.spec
    rpmbuild -ba --define "_topdir $RPMBUILD" $RPMBUILD/SPECS/scylla-tools.spec
fi
