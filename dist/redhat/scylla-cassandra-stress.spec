Name:           %{product}-cassandra-stress
Version:        %{version}
Release:        %{release}%{?dist}
Summary:        Scylla Cassandra Stress
Group:          Applications/Databases

License:        Apache
URL:            http://www.scylladb.com/
Source0:        %{reloc_pkg}
BuildArch:      noarch
Requires:       %{product}-conf %{product}-cassandra-stress-core
AutoReqProv:    no
Conflicts:      cassandra
Obsoletes:      %{product}-tools < 6.1

%description

%package core
License:        Apache
URL:            http://www.scylladb.com/
BuildArch:      noarch
Summary:        Core files for Scylla Cassandra Stress
Version:        %{version}
Release:        %{release}%{?dist}
Requires:       jre-11-headless
Obsoletes:      %{product}-tools-core < 6.1

%global __brp_python_bytecompile %{nil}
%global __brp_mangle_shebangs %{nil}

%description core
Core files for scylla casssandra stress.

%prep
%setup -q -n scylla-cassandra-stress


%build

%install
rm -rf $RPM_BUILD_ROOT
./install.sh --root "$RPM_BUILD_ROOT"

%files
/opt/scylladb/share/cassandra/bin/cassandra-stress
/opt/scylladb/share/cassandra/bin/cassandra-stressd
%{_bindir}/cassandra-stress
%{_bindir}/cassandra-stressd

%files core
%{_sysconfdir}/scylla/cassandra/cassandra-env.sh
%{_sysconfdir}/scylla/cassandra/logback.xml
%{_sysconfdir}/scylla/cassandra/logback-tools.xml
%{_sysconfdir}/scylla/cassandra/jvm*.options
/opt/scylladb/share/cassandra/bin/cassandra.in.sh
/opt/scylladb/share/cassandra/lib/*.jar
/opt/scylladb/share/cassandra/doc/cql3/CQL.css
/opt/scylladb/share/cassandra/doc/cql3/CQL.html

%changelog
* Fri Aug  7 2015 Takuya ASADA Takuya ASADA <syuu@cloudius-systems.com>
- inital version of scylla-tools.spec
