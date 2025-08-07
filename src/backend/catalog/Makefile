#-------------------------------------------------------------------------
#
# Makefile for backend/catalog
#
# src/backend/catalog/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/catalog
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global


OBJS = catalog.o dependency.o heap.o index.o cross_node_index.o indexing.o namespace.o aclchk.o \
       objectaccess.o objectaddress.o partition.o pg_aggregate.o pg_collation.o \
       pg_constraint.o pg_conversion.o \
       pg_depend.o pg_enum.o pg_inherits.o pg_largeobject.o pg_namespace.o \
       pg_operator.o pg_proc.o pg_publication.o pg_range.o \
	   pg_db_role_setting.o pg_shdepend.o pg_subscription.o pg_type.o \
	   pgxc_class.o storage.o toasting.o pgxc_shard_map.o local_statistics.o\
	   pg_profile.o pg_dblink.o pg_package.o pg_synonym.o pg_class.o pg_compile.o \
	   pg_term.o


OBJS += pg_extprotocol.o

BKIFILES = postgres.bki opentenbase_ora.bki all.bki postgres.description postgres.shdescription 

include $(top_srcdir)/src/backend/common.mk

all: $(BKIFILES) schemapg.h

# Note: there are some undocumented dependencies on the ordering in which
# the catalog header files are assembled into postgres.bki.  In particular,
# indexing.h had better be last, and toasting.h just before it.

POSTGRES_BKI_SRCS = $(addprefix $(top_srcdir)/src/include/catalog/,\
	pg_proc.h pg_type.h pg_attribute.h pg_class.h \
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.h \
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.h pg_amproc.h \
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h \
	pg_statistic_ext.h \
	pg_statistic.h pg_rewrite.h pg_trigger.h pg_event_trigger.h pg_description.h \
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h \
	pg_database.h pg_db_role_setting.h pg_tablespace.h pg_pltemplate.h \
	pg_authid.h pg_auth_history.h pg_auth_members.h pg_shdepend.h pg_shdescription.h \
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h \
	pg_ts_parser.h pg_ts_template.h pg_extension.h \
	pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h \
	pgxc_class.h pgxc_node.h pgxc_group.h \
	pg_foreign_table.h pg_policy.h pg_replication_origin.h \
	pg_default_acl.h pg_init_privs.h pg_seclabel.h pg_shseclabel.h \
	pg_collation.h pg_partitioned_table.h pg_range.h pg_transform.h pg_dbms_job.h \
	pg_sequence.h pg_ora_sequence.h pg_publication.h pg_publication_rel.h pg_publication_shard.h pg_subscription.h \
	pg_subscription_rel.h pg_subscription_shard.h pg_subscription_table.h pg_resgroup.h pg_resgroupcapability.h \
	toasting.h indexing.h pgxc_shard_map.h \
	pg_dblink.h pg_profile.h pg_profile_cost_weight.h pg_profile_user_password.h \
	audit/pg_audit_s.h audit/pg_audit_u.h audit/pg_audit_o.h audit/pg_audit_d.h audit/pg_audit_fga.h \
	colstore/pg_fn_register_info.h \
	mls/pg_cls_compartment.h mls/pg_cls_group.h mls/pg_cls_label.h \
    mls/pg_cls_level.h mls/pg_cls_table.h mls/pg_cls_policy.h mls/pg_cls_user.h \
    mls/pg_data_mask_map.h mls/pg_data_mask_user.h pg_package.h pg_synonym.h pg_childalias.h pg_compile.h pg_directory.h \
	pg_type_object.h  pg_spm_plan.h pg_spm_plan_history.h pg_term.h \
    )

# location of Catalog.pm
catalogdir = $(top_srcdir)/src/backend/catalog

# locations of headers that genbki.pl needs to read
pg_includes = -I$(top_srcdir)/src/include/catalog -I$(top_builddir)/src/include/catalog

# see explanation in ../parser/Makefile
postgres.description: postgres.bki ;

postgres.shdescription: postgres.bki ;

opentenbase_ora.bki : postgres.bki ;

schemapg.h: postgres.bki ;

# Technically, this should depend on Makefile.global, but then
# postgres.bki would need to be rebuilt after every configure run,
# even in distribution tarballs.  So this is cheating a bit, but it
# will achieve the goal of updating the version number when it
# changes.
postgres.bki: genbki.pl Catalog.pm $(POSTGRES_BKI_SRCS) $(top_srcdir)/configure $(top_srcdir)/src/include/catalog/duplicate_oids
	cd $(top_srcdir)/src/include/catalog && $(PERL) ./duplicate_oids
	$(PERL) -I $(catalogdir) $< $(pg_includes) --set-version=$(MAJORVERSION) $(POSTGRES_BKI_SRCS) -t postgres
	$(PERL) -I $(catalogdir) $< $(pg_includes) --set-version=$(MAJORVERSION) $(POSTGRES_BKI_SRCS) -t opentenbase_ora
# Generate schemapg.h for all the catalog, other files are unnecessary.
	$(PERL) -I $(catalogdir) $< $(pg_includes) --set-version=$(MAJORVERSION) $(POSTGRES_BKI_SRCS) -t all

.PHONY: install-data
install-data: $(BKIFILES) installdirs
	$(INSTALL_DATA) $(call vpathsearch,postgres.bki) '$(DESTDIR)$(datadir)/postgres.bki'
	$(INSTALL_DATA) $(call vpathsearch,postgres.description) '$(DESTDIR)$(datadir)/postgres.description'
	$(INSTALL_DATA) $(call vpathsearch,postgres.shdescription) '$(DESTDIR)$(datadir)/postgres.shdescription'
	$(INSTALL_DATA) $(call vpathsearch,opentenbase_ora.bki) '$(DESTDIR)$(datadir)/opentenbase_ora.bki'
	$(INSTALL_DATA) $(call vpathsearch,opentenbase_ora.description) '$(DESTDIR)$(datadir)/opentenbase_ora.description'
	$(INSTALL_DATA) $(call vpathsearch,opentenbase_ora.shdescription) '$(DESTDIR)$(datadir)/opentenbase_ora.shdescription'
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(srcdir)/system_views_pg.sql '$(DESTDIR)$(datadir)/system_views_pg.sql'
	$(INSTALL_DATA) $(srcdir)/system_views_opentenbase_ora.sql '$(DESTDIR)$(datadir)/system_views_opentenbase_ora.sql'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, $(BKIFILES) system_views.sql system_views_pg.sql system_views_opentenbase_ora.sql information_schema.sql sql_features.txt)

# postgres.bki, postgres.description, postgres.shdescription, and schemapg.h
# are in the distribution tarball, so they are not cleaned here.
clean:

maintainer-clean: clean
	rm -f $(BKIFILES)
