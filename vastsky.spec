%define vas_inst_root /opt/vas
%define vas_version 2.1
%define vas_release 3

Summary: Vastsky
Name: vastsky
Version: %{vas_version}
Release: %{vas_release}
Group: System Environment/Daemons
Source:	vastsky-%{vas_version}.tar.gz
Vendor: Vastsky project
URL: http://sourceforge.net/projects/vastsky/
Requires: python
BuildRequires: python
Prefix: %{_prefix}
BuildRoot: %{_tmppath}/%{name}-root
BuildArch: noarch
License: BSD

%description
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.

%prep
%setup -q

%build

%install
rm -rf ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}%{vas_inst_root}
mkdir -p ${RPM_BUILD_ROOT}%{_sysconfdir}
mkdir -p ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d
mkdir -p ${RPM_BUILD_ROOT}%{_bindir}
mkdir -p ${RPM_BUILD_ROOT}%{_sbindir}
sh -c "cd src && ./test_install.py --root=${RPM_BUILD_ROOT}"
cp examples/vas.conf ${RPM_BUILD_ROOT}%{_sysconfdir}
mkdir -p "${RPM_BUILD_ROOT}/usr/share/doc/vastsky/"
cp doc/vas_*.txt "${RPM_BUILD_ROOT}/usr/share/doc/vastsky/"

%clean
rm -rf ${RPM_BUILD_ROOT}

#%files

%package common
Summary: Vastsky common
Group: System Environment/Daemons
Requires: python

%description common
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.
vastsky-common package includes the code commonly used among other vastsky
packages.

%files common
%defattr(-,root,root)
%dir %{vas_inst_root}
%dir %{vas_inst_root}/lib
%dir %{vas_inst_root}/bin
%dir %{_localstatedir}/lib/vas
%dir %{_localstatedir}/lib/vas/db
%{vas_inst_root}/lib/daemon_launcher.pyc
%{vas_inst_root}/lib/vas_conf.pyc
%{vas_inst_root}/lib/vas_db.pyc
%{vas_inst_root}/lib/vas_subr.pyc
%{vas_inst_root}/bin/daemon_launcher
%config(noreplace) %{_sysconfdir}/vas.conf

%package doc
Summary: Vastsky documentation
Group: System Environment/Daemons

%description doc
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.
vastsky-doc package includes documentations for vastsky.

%files doc
%dir /usr/share/doc/vastsky
/usr/share/doc/vastsky/vas_admin.txt
/usr/share/doc/vastsky/vas_api.txt
/usr/share/doc/vastsky/vas_examples.txt
/usr/share/doc/vastsky/vas_cli.txt
/usr/share/doc/vastsky/vas_install.txt
/usr/share/doc/vastsky/vas_overview.txt
/usr/share/doc/vastsky/vas_roadmap.txt

%package sm
Summary: Vastsky storage manager
Group: System Environment/Daemons
Requires: vastsky-common = %{vas_version}

%description sm
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.
vastsky-sm package includes vastsky storage manager.

%post sm
/sbin/chkconfig --add vas_sm

%preun sm
if [ $1 = 0 ]; then
	/sbin/chkconfig --del vas_sm
fi

%files sm
%defattr(-,root,root)
%dir %{_localstatedir}/lib/vas/db
%{_sysconfdir}/init.d/vas_sm
%{vas_inst_root}/bin/storage_manager
%{vas_inst_root}/bin/shutdownAll
%{vas_inst_root}/bin/check_servers
%{vas_inst_root}/lib/storage_manager.pyc
%{vas_inst_root}/lib/shutdownAll.pyc
%{vas_inst_root}/lib/check_servers.pyc
%{vas_inst_root}/bin/vas_db

%package hsvr
Summary: Vastsky head server
Group: System Environment/Daemons
Requires: vastsky-common = %{vas_version}

%description hsvr
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.
vastsky-hsvr package includes vastsky head server.

%post hsvr
/sbin/chkconfig --add vas_hsvr

%preun hsvr
if [ $1 = 0 ]; then
	/sbin/chkconfig --del vas_hsvr
fi

%files hsvr
%defattr(-,root,root)
%{_sysconfdir}/init.d/vas_hsvr
%{vas_inst_root}/lib/hsvr_reporter.pyc
%{vas_inst_root}/lib/hsvr_agent.pyc
%{vas_inst_root}/lib/lvol_error.pyc
%{vas_inst_root}/lib/mdstat.pyc
%{vas_inst_root}/lib/mdadm_event.pyc
%{vas_inst_root}/bin/hsvr_reporter
%{vas_inst_root}/bin/hsvr_agent
%{vas_inst_root}/bin/lvol_error
%{vas_inst_root}/bin/mdadm_event

%package ssvr
Summary: Vastsky storage server
Group: System Environment/Daemons
Requires: vastsky-common = %{vas_version}

%description ssvr
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.
vastsky-ssvr package includes vastsky storage server.

%post ssvr
/sbin/chkconfig --add vas_ssvr

%preun ssvr
if [ $1 = 0 ]; then
	/sbin/chkconfig --del vas_ssvr
fi

%files ssvr
%defattr(-,root,root)
%{_sysconfdir}/init.d/vas_ssvr
%{vas_inst_root}/lib/ssvr_reporter.pyc
%{vas_inst_root}/lib/ssvr_agent.pyc
%{vas_inst_root}/lib/DiskPatroller.pyc
%{vas_inst_root}/bin/ssvr_reporter
%{vas_inst_root}/bin/ssvr_agent
%{vas_inst_root}/bin/DiskPatroller

%package cli
Summary: Vastsky user commands
Group: System Environment/Daemons
Requires: vastsky-common = %{vas_version}

%description cli
Vastsky is a linux-based cluster storage system, which provides logical
volumes (linux block devices) to users by aggregating disks over a network.
vastsky-cli package includes vastsky command line interface to talk with
the vastsky storage manager.

%post cli

%preun cli

%files cli
%defattr(-,root,root)
%{_bindir}/hsvr_list
%{_bindir}/lvol_attach
%{_bindir}/lvol_create
%{_bindir}/lvol_delete
%{_bindir}/lvol_detach
%{_bindir}/lvol_list
%{_bindir}/lvol_show
%{_bindir}/pdsk_list
%{_bindir}/ssvr_list
%{_sbindir}/hsvr_delete
%{_sbindir}/pdsk_delete
%{_sbindir}/ssvr_delete
%{vas_inst_root}/lib/hsvr_list.pyc
%{vas_inst_root}/lib/lvol_attach.pyc
%{vas_inst_root}/lib/lvol_create.pyc
%{vas_inst_root}/lib/lvol_delete.pyc
%{vas_inst_root}/lib/lvol_detach.pyc
%{vas_inst_root}/lib/lvol_list.pyc
%{vas_inst_root}/lib/lvol_show.pyc
%{vas_inst_root}/lib/pdsk_list.pyc
%{vas_inst_root}/lib/ssvr_list.pyc
%{vas_inst_root}/lib/hsvr_delete.pyc
%{vas_inst_root}/lib/pdsk_delete.pyc
%{vas_inst_root}/lib/ssvr_delete.pyc
