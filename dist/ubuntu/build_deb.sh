#!/bin/bash -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    rm -rf debian build conf/hotspot_compiler
    mkdir build
fi

DISTRIBUTION=`lsb_release -i|awk '{print $3}'`
RELEASE=`lsb_release -r|awk '{print $2}'`
CODENAME=`lsb_release -c|awk '{print $2}'`

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
if [ "$SCYLLA_VERSION" = "development" ]; then
	SCYLLA_VERSION=0development
fi
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-tools ../scylla-tools_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/ubuntu/debian debian

cp dist/ubuntu/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$CODENAME/g" debian/changelog

sudo apt-get -y install debhelper openjdk-7-jdk ant ant-optional python-support dpatch bash-completion devscripts

debuild -r fakeroot --no-tgz-check -us -uc
