%define         project hamgr
%define         summary High Availability Manager for OpenStack
%define         daemons server
%define         _unpackaged_files_terminate_build 0

Name:           %{_package}
Version:        %{_version}
Release:        %{_release}
Summary:        %{summary}

License:        Apache 2.0
URL:            http://www.platform9.com

AutoReqProv:    no
Provides:       %{_package}
BuildArch:      %{_arch}

BuildRequires:  python-devel
BuildRequires:  libffi-devel
BuildRequires:  mysql-devel

Requires(pre): /usr/sbin/useradd, /usr/bin/getent
Requires(postun): /usr/sbin/userdel

# ignore byte compile for python 3.5 based libraries when build with python 2.7
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')

%description
Distribution of the %{summary} built from %{project}@%{_githash}

%prep
# expand into BUILD
tar xf %{_sourcedir}/source.tar

%build

%install

# virtualenv and setup
virtualenv -p python3 %{buildroot}/opt/pf9/%{project}

%{buildroot}/opt/pf9/%{project}/bin/python %{buildroot}/opt/pf9/%{project}/bin/pip install -U pip==20.2.4 pbr==3.1.1 setuptools

# setup.py install with pbr version 1.8.1 does not seem to collect the requirements to
# site-packages. Just pip install of root directory seems to fix the issue
%{buildroot}/opt/pf9/%{project}/bin/python %{buildroot}/opt/pf9/%{project}/bin/pip \
    install -c https://raw.githubusercontent.com/openstack/requirements/stable/rocky/upper-constraints.txt .

# Following should be removed when pf9-ha is upgraded to stable/stein
%{buildroot}/opt/pf9/%{project}/bin/python %{buildroot}/opt/pf9/%{project}/bin/pip \
    install -c https://raw.githubusercontent.com/openstack/requirements/stable/rocky/upper-constraints.txt eventlet==0.24.1

# tests
rm -rf %{buildroot}/opt/pf9/%{project}/lib/python?.?/site-packages/%{project}/tests
rm -rf %{buildroot}/opt/pf9/%{project}/lib/python?.?/site-packages/shared/tests

# Migrate repo config
install -p -t  %{buildroot}/opt/pf9/%{project}/lib/python?.?/site-packages/%{project}/db/ %{_builddir}/%{project}/db/migrate.cfg

# init scripts
for daemon in %{daemons}
do
    initscript=%{buildroot}/%{_initrddir}/pf9-%{project}-$daemon
    install -p -D -m 755 etc/init.d/%{project}.template $initscript
    sed -i "s/suffix=.*/suffix=$daemon/" $initscript
    sed -i "s/user=.*/user=%{_svcuser}/" $initscript
done

# config files
install -d -m 755 %{buildroot}%{_sysconfdir}/pf9/%{project}
install -p -m 640 -t %{buildroot}%{_sysconfdir}/pf9/%{project}/ \
                     etc/*.conf etc/*.ini
# Utils
install -p -m 755 -t %{buildroot}/opt/pf9/%{project}/bin/ \
                     bin/*
# log directory
install -d -m 755 %{buildroot}%{_localstatedir}/log/pf9/%{project}

# pid directory
install -d -m 755 %{buildroot}%{_localstatedir}/run/%{project}

# logrotate
install -p -D -m 644 etc/logrotate.hamgr  %{buildroot}%{_sysconfdir}/logrotate.d/pf9-%{project}

%clean

%files
%defattr(-,%{_svcuser},%{_svcgroup},-)

# include logrotate config file
%attr(0644, root, root) %{_sysconfdir}/logrotate.d/pf9-%{project}

# the virtualenv
%dir /opt/pf9/%{project}
/opt/pf9/%{project}

# services
%{_initrddir}/pf9-%{project}-*


# /etc/project config files
%dir %{_sysconfdir}/pf9/%{project}
%config(noreplace) %attr(-, %{_svcuser}, %{_svcgroup}) %{_sysconfdir}/pf9/%{project}/*.conf
%config(noreplace) %attr(-, %{_svcuser}, %{_svcgroup}) %{_sysconfdir}/pf9/%{project}/*.ini

# /var/log
%dir %attr(0755, %{_svcuser}, %{_svcgroup}) %{_localstatedir}/log/pf9/%{project}

# /var/run (for pidfile)
%dir %attr(0755, %{_svcuser}, %{_svcgroup}) %{_localstatedir}/run/%{project}

%pre
/usr/bin/getent group %{_svcgroup} || \
    /usr/sbin/groupadd -r %{_svcgroup}
/usr/bin/getent passwd %{_svcuser} || \
    /usr/sbin/useradd -r \
                      -d / \
                      -s /sbin/nologin \
                      -g %{_svcgroup} \
                      %{_svcuser}

%post

%preun
if [ $1 = 0 ] # package is being erased, not upgraded
then
    for daemon in %{daemons}
    do
        /sbin/service pf9-%{project}-$daemon stop >/dev/null 2>&1
    done
fi

%postun
if [ $1 -ge 1 ]
then
    # Package upgrade, not uninstall
    for daemon in %{daemons}
    do
        /sbin/service pf9-%{project}-$daemon condrestart > /dev/null 2>&1 || :
    done
elif [ "%{_package}" == "hamgr" ]
then
    # uninstalling from controller, remove hamgr user.
    /usr/sbin/userdel %{_svcuser}
fi


%changelog

