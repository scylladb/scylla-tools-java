#!/bin/bash -e

if [ ! -e dist/debian/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    rm -rf debian build conf/hotspot_compiler
    mkdir build
fi
sudo apt-get -y update
if [ ! -f /usr/bin/git ]; then
    sudo apt-get -y install git
fi
if [ ! -f /usr/bin/lsb_release ]; then
    sudo apt-get -y install lsb-release
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

cp -a dist/debian/debian debian

cp dist/debian/changelog.in debian/changelog
cp dist/debian/control.in debian/control
if [ "$DISTRIBUTION" = "Ubuntu" ]; then
    sed -i -e "s/@@REVISION@@/0ubuntu1/g" debian/changelog
else
    sed -i -e "s/@@REVISION@@/1/g" debian/changelog
fi
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$CODENAME/g" debian/changelog

if [ "$CODENAME" = "trusty" ]; then
    sudo apt-get -y install software-properties-common
    sudo add-apt-repository -y ppa:openjdk-r/ppa
    sudo apt-get -y update
    sed -i -e "s/@@BUILD_DEPENDS@@/python-support (>= 0.90.0)/g" debian/control
    sudo apt-get -y install python-support
elif [ "$CODENAME" = "jessie" ]; then
    sudo sh -c 'echo deb "http://httpredir.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list'
    sudo apt-get -y update
    sudo apt-get install -t jessie-backports -y ca-certificates-java
    sed -i -e "s/@@BUILD_DEPENDS@@/python-support (>= 0.90.0)/g" debian/control
    sudo apt-get -y install python-support
else
    sed -i -e "s/@@BUILD_DEPENDS@@//g" debian/control
fi

sudo apt-get install -y debhelper openjdk-8-jre-headless openjdk-8-jdk-headless ant ant-optional bash-completion python devscripts
sudo /usr/bin/update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
debuild -r fakeroot --no-tgz-check -us -uc
