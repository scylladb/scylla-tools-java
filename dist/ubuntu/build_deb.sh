#!/bin/bash -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
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
if [ ! -f /usr/bin/mk-build-deps ]; then
    sudo apt-get -y install devscripts
fi
if [ ! -f /usr/bin/equivs-build ]; then
    sudo apt-get -y install equivs
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

cp -a dist/ubuntu/debian debian

cp dist/ubuntu/changelog.in debian/changelog
cp dist/ubuntu/control.in debian/control
cp dist/ubuntu/rules.in debian/rules
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog
sed -i -e "s/@@CODENAME@@/$CODENAME/g" debian/changelog
if [ "$RELEASE" != "16.04" ]; then
    sed -i -e "s/@@BUILD_DEPENDS@@/python-support (>= 0.90.0), openjdk-7-jdk/g" debian/control
    sudo apt-get -y install python-support
    sed -i -e "s#@@JAVA_HOME@@#/usr/lib/jvm/java-7-openjdk-amd64#g" debian/rules
else
    sed -i -e "s/@@BUILD_DEPENDS@@/openjdk-8-jdk-headless/g" debian/control
    sed -i -e "s#@@JAVA_HOME@@#/usr/lib/jvm/java-8-openjdk-amd64#g" debian/rules
fi

echo Y | sudo mk-build-deps -i -r
debuild -r fakeroot --no-tgz-check -us -uc
