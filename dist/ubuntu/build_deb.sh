#!/bin/sh -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

sudo apt-get -y install debhelper openjdk-7-jdk ant ant-optional python-support dpatch bash-completion devscripts

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
if [ "$SCYLLA_VERSION" = "development" ]; then
	SCYLLA_VERSION=0development
fi
cp dist/ubuntu/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog

debuild -r fakeroot --no-tgz-check -us -uc
