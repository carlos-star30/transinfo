# trans_fields_mapping 表定义

- 生成时间: 2026-03-31 14:52:12
- 数据来源: 本地 MySQL 数据库 `trans_fields_mapping`
- 表数量: 29

## 表清单

- `auth_audit_logs`
- `bw_object_name`
- `dd02t`
- `dd03l`
- `dd03t`
- `dd04t`
- `import_status`
- `rsdiobj`
- `rsdiobjt`
- `RSDS`
- `rsdssegfd`
- `rsdssegfdt`
- `RSDST`
- `rsksfieldnew`
- `rsksfieldnewt`
- `rsksnew`
- `rsksnewt`
- `rsoadso`
- `RSOADSOT`
- `RSTRAN`
- `rstran_mapping_rule`
- `rstran_mapping_rule_full`
- `RSTRANFIELD`
- `RSTRANRULE`
- `rstranstepcnst`
- `rstransteprout`
- `user_hidden_object`
- `user_sessions`
- `users`

## `auth_audit_logs`

```sql
CREATE TABLE `auth_audit_logs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `event_type` varchar(64) NOT NULL,
  `username` varchar(64) DEFAULT NULL,
  `actor` varchar(64) DEFAULT NULL,
  `success` tinyint(1) NOT NULL DEFAULT '1',
  `detail` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_auth_event` (`event_type`,`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=107 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

## `bw_object_name`

```sql
CREATE TABLE `bw_object_name` (
  `BW_OBJECT` varchar(40) NOT NULL COMMENT 'BW object',
  `BW_OBJECT_NORM` varchar(40) NOT NULL COMMENT 'BW object normalized to uppercase',
  `SOURCESYS` varchar(25) DEFAULT NULL COMMENT 'Source System',
  `BW_OBJECT_TYPE` varchar(20) DEFAULT NULL COMMENT 'BW object type',
  `NAME_EN` varchar(255) DEFAULT NULL COMMENT 'Object Name (EN)',
  `NAME_DE` varchar(255) DEFAULT NULL COMMENT 'Object Name (DE)',
  `NAME_EN_NORM` varchar(255) DEFAULT NULL COMMENT 'Object Name (EN) normalized to uppercase',
  `NAME_DE_NORM` varchar(255) DEFAULT NULL COMMENT 'Object Name (DE) normalized to uppercase',
  KEY `idx_bw_object_sourcesys` (`BW_OBJECT`,`SOURCESYS`),
  KEY `idx_bw_object_lookup` (`BW_OBJECT`,`BW_OBJECT_TYPE`,`SOURCESYS`),
  KEY `idx_bw_object_norm_sourcesys` (`BW_OBJECT_NORM`,`SOURCESYS`),
  KEY `idx_bw_object_norm_lookup` (`BW_OBJECT_NORM`,`BW_OBJECT_TYPE`,`SOURCESYS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

## `dd02t`

```sql
CREATE TABLE `dd02t` (
  `TABNAME` char(30) NOT NULL COMMENT 'TABNAME',
  `DDLANGUAGE` char(2) NOT NULL COMMENT 'DDLANGUAGE',
  `AS4LOCAL` char(1) NOT NULL COMMENT 'AS4LOCAL',
  `AS4VERS` char(4) NOT NULL COMMENT 'AS4VERS',
  `DDTEXT` char(61) DEFAULT NULL COMMENT 'DDTEXT',
  PRIMARY KEY (`TABNAME`,`DDLANGUAGE`,`AS4LOCAL`,`AS4VERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='DD02T table text metadata'
```

## `dd03l`

```sql
CREATE TABLE `dd03l` (
  `TABNAME` char(30) NOT NULL COMMENT 'TABNAME',
  `FIELDNAME` char(30) NOT NULL COMMENT 'FIELDNAME',
  `AS4LOCAL` char(1) NOT NULL COMMENT 'AS4LOCAL',
  `AS4VERS` char(4) NOT NULL COMMENT 'AS4VERS',
  `POSITION` char(4) NOT NULL COMMENT 'POSITION',
  `KEYFLAG` char(1) DEFAULT NULL COMMENT 'KEYFLAG',
  `MANDATORY` char(1) DEFAULT NULL COMMENT 'MANDATORY',
  `ROLLNAME` char(30) DEFAULT NULL COMMENT 'ROLLNAME',
  `CHECKTABLE` char(30) DEFAULT NULL COMMENT 'CHECKTABLE',
  `ADMINFIELD` char(3) DEFAULT NULL COMMENT 'ADMINFIELD',
  `INTTYPE` char(1) DEFAULT NULL COMMENT 'INTTYPE',
  `INTLEN` char(6) DEFAULT NULL COMMENT 'INTLEN',
  `REFTABLE` char(30) DEFAULT NULL COMMENT 'REFTABLE',
  `PRECFIELD` char(30) DEFAULT NULL COMMENT 'PRECFIELD',
  `REFFIELD` char(30) DEFAULT NULL COMMENT 'REFFIELD',
  `CONROUT` char(10) DEFAULT NULL COMMENT 'CONROUT',
  `NOTNULL` char(1) DEFAULT NULL COMMENT 'NOTNULL',
  `DATATYPE` char(10) DEFAULT NULL COMMENT 'DATATYPE',
  `LENG` char(6) DEFAULT NULL COMMENT 'LENG',
  `DECIMALS` char(6) DEFAULT NULL COMMENT 'DECIMALS',
  `DOMNAME` char(30) DEFAULT NULL COMMENT 'DOMNAME',
  `SHLPORIGIN` char(1) DEFAULT NULL COMMENT 'SHLPORIGIN',
  `TABLETYPE` char(1) DEFAULT NULL COMMENT 'TABLETYPE',
  `DEPTH` char(2) DEFAULT NULL COMMENT 'DEPTH',
  `COMPTYPE` char(1) DEFAULT NULL COMMENT 'COMPTYPE',
  `REFTYPE` char(1) DEFAULT NULL COMMENT 'REFTYPE',
  `LANGUFLAG` char(1) DEFAULT NULL COMMENT 'LANGUFLAG',
  `DBPOSITION` char(4) NOT NULL COMMENT 'DBPOSITION',
  `ANONYMOUS` char(1) NOT NULL COMMENT 'ANONYMOUS',
  `OUTPUTSTYLE` char(2) NOT NULL COMMENT 'OUTPUTSTYLE',
  `SRS_ID` int DEFAULT NULL COMMENT 'SRS_ID',
  PRIMARY KEY (`TABNAME`,`FIELDNAME`,`AS4LOCAL`,`AS4VERS`,`POSITION`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='DD03L table field metadata'
```

## `dd03t`

```sql
CREATE TABLE `dd03t` (
  `TABNAME` char(30) NOT NULL COMMENT 'TABNAME',
  `DDLANGUAGE` char(2) NOT NULL COMMENT 'DDLANGUAGE',
  `AS4LOCAL` char(1) NOT NULL COMMENT 'AS4LOCAL',
  `FIELDNAME` char(30) NOT NULL COMMENT 'FIELDNAME',
  `DDTEXT` char(60) NOT NULL COMMENT 'DDTEXT',
  PRIMARY KEY (`TABNAME`,`DDLANGUAGE`,`AS4LOCAL`,`FIELDNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='DD03T field text metadata'
```

## `dd04t`

```sql
CREATE TABLE `dd04t` (
  `ROLLNAME` char(30) NOT NULL COMMENT 'ROLLNAME',
  `DDLANGUAGE` char(2) NOT NULL COMMENT 'DDLANGUAGE',
  `AS4LOCAL` char(1) NOT NULL COMMENT 'AS4LOCAL',
  `AS4VERS` char(4) NOT NULL COMMENT 'AS4VERS',
  `DDTEXT` char(60) DEFAULT NULL COMMENT 'DDTEXT',
  `REPTEXT` char(55) DEFAULT NULL COMMENT 'REPTEXT',
  `SCRTEXT_S` char(10) DEFAULT NULL COMMENT 'SCRTEXT_S',
  `SCRTEXT_M` char(20) DEFAULT NULL COMMENT 'SCRTEXT_M',
  `SCRTEXT_L` char(40) DEFAULT NULL COMMENT 'SCRTEXT_L',
  PRIMARY KEY (`ROLLNAME`,`DDLANGUAGE`,`AS4LOCAL`,`AS4VERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='DD04T data element text metadata'
```

## `import_status`

```sql
CREATE TABLE `import_status` (
  `table_name` varchar(64) NOT NULL,
  `last_import_at` datetime NOT NULL,
  `last_import_count` int NOT NULL DEFAULT '0',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

## `rsdiobj`

```sql
CREATE TABLE `rsdiobj` (
  `IOBJNM` char(30) NOT NULL COMMENT 'IOBJNM',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `IOBJTP` char(3) DEFAULT NULL COMMENT 'IOBJTP',
  `OBJSTAT` char(3) DEFAULT NULL COMMENT 'OBJSTAT',
  `CONTREL` char(6) DEFAULT NULL COMMENT 'CONTREL',
  `CONTTIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'CONTTIMESTMP',
  `OWNER` char(12) DEFAULT NULL COMMENT 'OWNER',
  `BWAPPL` char(10) DEFAULT NULL COMMENT 'BWAPPL',
  `ACTIVFL` char(1) DEFAULT NULL COMMENT 'ACTIVFL',
  `ABAP_LANGUAGE_VERSION` char(1) DEFAULT NULL COMMENT 'ABAP_LANGUAGE_VERSION',
  `PROTECFL` char(1) DEFAULT NULL COMMENT 'PROTECFL',
  `PRIVATEFL` char(1) DEFAULT NULL COMMENT 'PRIVATEFL',
  `FIELDNM` char(30) DEFAULT NULL COMMENT 'FIELDNM',
  `ATRONLYFL` char(1) DEFAULT NULL COMMENT 'ATRONLYFL',
  `BCTCOMP` char(10) DEFAULT NULL COMMENT 'BCTCOMP',
  `BDSFL` char(1) DEFAULT NULL COMMENT 'BDSFL',
  `TSTPNM` char(12) DEFAULT NULL COMMENT 'TSTPNM',
  `TIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'TIMESTMP',
  `ORIGIN` char(1) DEFAULT NULL COMMENT 'ORIGIN',
  `MTINFOAREA` char(30) DEFAULT NULL COMMENT 'MTINFOAREA',
  `MTMODELVERSION` char(8) DEFAULT NULL COMMENT 'MTMODELVERSION',
  `TXTSH_SET` char(1) DEFAULT NULL COMMENT 'TXTSH_SET',
  `RAL_ENABLED` char(1) DEFAULT NULL COMMENT 'RAL_ENABLED',
  PRIMARY KEY (`IOBJNM`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='InfoObject properties metadata'
```

## `rsdiobjt`

```sql
CREATE TABLE `rsdiobjt` (
  `LANGU` char(2) NOT NULL COMMENT 'LANGU',
  `IOBJNM` char(30) NOT NULL COMMENT 'IOBJNM',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `TXTSH` char(21) DEFAULT NULL COMMENT 'TXTSH',
  `TXTLG` char(60) DEFAULT NULL COMMENT 'TXTLG',
  PRIMARY KEY (`LANGU`,`IOBJNM`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='InfoObject text metadata'
```

## `RSDS`

```sql
CREATE TABLE `RSDS` (
  `DATASOURCE` char(30) NOT NULL COMMENT 'DATASOURCE',
  `LOGSYS` char(10) NOT NULL COMMENT 'Source system',
  `OBJVERS` char(1) NOT NULL COMMENT 'Object version',
  `OBJSTAT` char(3) DEFAULT NULL COMMENT 'Object Status',
  `ACTIVFL` char(1) DEFAULT NULL COMMENT 'Active and revised version do not agree',
  `TYPE` char(1) DEFAULT NULL COMMENT 'Data Type of a DataSource',
  `PRIMSEGID` char(4) DEFAULT NULL COMMENT 'Primary Segment',
  `OBJECTFD` char(30) DEFAULT NULL COMMENT 'Field Name: Object ID',
  `APPLNM` char(30) DEFAULT NULL COMMENT 'Application Component',
  `BASOSOURCE` char(30) DEFAULT NULL COMMENT 'BASOSOURCE',
  `DELTA` char(4) DEFAULT NULL COMMENT 'DELTA',
  `STOCKUPD` char(1) DEFAULT NULL COMMENT 'Flag: Creation of opening balance is supported',
  `PACKGUPD` char(1) DEFAULT NULL COMMENT 'Repeated request of a data packet supported',
  `REALTIME` char(1) DEFAULT NULL COMMENT 'Real-Time Property of a DataSource',
  `TIMDEPFL` char(1) DEFAULT NULL COMMENT 'Data Is Time Dependent',
  `LANGUDEPFL` char(1) DEFAULT NULL COMMENT 'Data Is Language Dependent',
  `EXSTRUCTURE` char(30) DEFAULT NULL COMMENT 'EXSTRUCTURE',
  `VIRTCUBE` char(1) DEFAULT NULL COMMENT 'DataSource: Extractor Supports Direct Access',
  `HYBRIDACCESS` char(1) DEFAULT NULL COMMENT 'DataSource: HybridProvider Access Supported',
  `CHARONLY` char(1) DEFAULT NULL COMMENT 'Structure only allows character fields',
  `TSTMPOLTP` decimal(15,0) DEFAULT NULL COMMENT 'UTC Time Stamp in Short Form (YYYYMMDDhhmmss)',
  `DELTAACT` char(1) DEFAULT NULL COMMENT 'Boolean',
  `ICON` char(4) DEFAULT NULL COMMENT 'ICON',
  `CONTREL` char(6) DEFAULT NULL COMMENT 'Content release',
  `CONTTIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'Content time stamp: Last modification to the object by SAP',
  `APPL_CALLBACK` char(30) DEFAULT NULL COMMENT 'Function Module Rule Proposal',
  `TFMETHODS` char(1) DEFAULT NULL COMMENT 'Supported transfer methods',
  `ARCHMETHOD` char(1) DEFAULT NULL COMMENT 'Archive Link of a DataSource',
  `DUPREC` char(1) DEFAULT NULL COMMENT 'Flag: DataSource returns duplicate records',
  `INITSIMU` char(1) DEFAULT NULL COMMENT 'Delta process initialization simulation',
  `ZDD_ABLE` char(1) DEFAULT NULL COMMENT 'Boolean',
  `RECONCILIATION` char(1) DEFAULT NULL COMMENT 'DataSource for Data Reconciliation',
  `CHAR_PSA` char(1) DEFAULT NULL COMMENT 'PSA for All Segments in CHAR Format',
  `CHAR1000` char(1) DEFAULT NULL COMMENT 'Thousand separator',
  `DEZICHAR` char(1) DEFAULT NULL COMMENT 'Character for decimal point',
  `CONTPACKET` char(30) DEFAULT NULL COMMENT 'Package',
  `CONTMASTER` char(40) DEFAULT NULL COMMENT 'CONTMASTER',
  `DELTAFD` char(30) DEFAULT NULL COMMENT 'Delta-Relevant Field of DataSource',
  `DELTATP` char(1) DEFAULT NULL COMMENT 'Type of Delta-Relevant Field',
  `DELTALOW` char(45) DEFAULT NULL COMMENT 'Safety Interval Lower Limit',
  `DELTAHIGH` char(45) DEFAULT NULL COMMENT 'Safety Interval Upper Limit of Delta Selection',
  `DELTATZ` char(6) DEFAULT NULL COMMENT 'DELTATZ',
  `TSTPNM` char(12) DEFAULT NULL COMMENT 'Last Changed By',
  `TIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'UTC Time Stamp in Short Form (YYYYMMDDhhmmss)',
  `CODEID` char(25) DEFAULT NULL COMMENT 'ID of the Generated Program',
  `TRANSTRUS_APPL` char(30) DEFAULT NULL COMMENT 'Deep Structure: Application Structures of a DataSource',
  `TRANSTRUS_CHAR` char(30) DEFAULT NULL COMMENT 'Deep Structure: DataSource Character Structures',
  `HASHNUMBER` char(5) DEFAULT NULL COMMENT 'Number of Hash Name for DataSource',
  `CONTSRCTYPE` char(3) DEFAULT NULL COMMENT 'Content Release Type',
  `CONTSRCVERS` char(6) DEFAULT NULL COMMENT 'Content Version of DataSource',
  `PSADATCLS` char(5) DEFAULT NULL COMMENT 'Data Class for PSA Table',
  `PSASIZCAT` char(2) DEFAULT NULL COMMENT 'Size Category for PSA Table',
  `CONVLANGU` char(2) DEFAULT NULL COMMENT 'Conversion Language',
  `EXTTABL` char(1) DEFAULT NULL COMMENT 'Table should be generated as extended table',
  PRIMARY KEY (`DATASOURCE`,`LOGSYS`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='DataSource in BW'
```

## `rsdssegfd`

```sql
CREATE TABLE `rsdssegfd` (
  `DATASOURCE` char(30) NOT NULL COMMENT 'DATASOURCE',
  `LOGSYS` char(10) NOT NULL COMMENT 'LOGSYS',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `SEGID` char(4) NOT NULL COMMENT 'SEGID',
  `POSIT` char(4) NOT NULL COMMENT 'POSIT',
  `FIELDNM` char(30) DEFAULT NULL COMMENT 'FIELDNM',
  `DTELNM` char(30) DEFAULT NULL COMMENT 'DTELNM',
  `DATATYPE` char(10) DEFAULT NULL COMMENT 'DATATYPE',
  `LENG` char(6) DEFAULT NULL COMMENT 'LENG',
  `CONVEXIT` char(5) DEFAULT NULL COMMENT 'CONVEXIT',
  `DECIMALS` char(6) DEFAULT NULL COMMENT 'DECIMALS',
  `UNIFIELDNM` char(30) DEFAULT NULL COMMENT 'UNIFIELDNM',
  `CONSTANT` char(60) DEFAULT NULL COMMENT 'CONSTANT',
  `DOMANM` char(30) DEFAULT NULL COMMENT 'DOMANM',
  `KEYFIELD` char(1) DEFAULT NULL COMMENT 'KEYFIELD',
  `SELECTION` char(1) DEFAULT NULL COMMENT 'SELECTION',
  `SELOPTS` int DEFAULT NULL COMMENT 'SELOPTS',
  `SELDIRECT` char(1) DEFAULT NULL COMMENT 'SELDIRECT',
  `LOWERCASE` char(1) DEFAULT NULL COMMENT 'LOWERCASE',
  `OUTPUTLEN` char(6) DEFAULT NULL COMMENT 'OUTPUTLEN',
  `IOBJNM` char(30) DEFAULT NULL COMMENT 'IOBJNM',
  `CONVEXITSRC` char(5) DEFAULT NULL COMMENT 'CONVEXITSRC',
  `CONVTYPE` char(1) DEFAULT NULL COMMENT 'CONVTYPE',
  `TRANSFER` char(1) DEFAULT NULL COMMENT 'TRANSFER',
  `CONTOBJECT` char(40) DEFAULT NULL COMMENT 'CONTOBJECT',
  `ORIGIN` char(30) DEFAULT NULL COMMENT 'ORIGIN',
  PRIMARY KEY (`DATASOURCE`,`LOGSYS`,`OBJVERS`,`SEGID`,`POSIT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='RSDS segment field metadata'
```

## `rsdssegfdt`

```sql
CREATE TABLE `rsdssegfdt` (
  `LANGU` char(2) NOT NULL COMMENT 'LANGU',
  `DATASOURCE` char(30) NOT NULL COMMENT 'DATASOURCE',
  `LOGSYS` char(10) NOT NULL COMMENT 'LOGSYS',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `SEGID` char(4) NOT NULL COMMENT 'SEGID',
  `POSIT` char(4) NOT NULL COMMENT 'POSIT',
  `FIELDNM` char(30) NOT NULL COMMENT 'FIELDNM',
  `TXTSH` char(22) DEFAULT NULL COMMENT 'TXTSH',
  `TXTMD` char(40) DEFAULT NULL COMMENT 'TXTMD',
  `TXTLG` char(60) DEFAULT NULL COMMENT 'TXTLG',
  PRIMARY KEY (`LANGU`,`DATASOURCE`,`LOGSYS`,`OBJVERS`,`SEGID`,`POSIT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='RSDS segment field text metadata'
```

## `RSDST`

```sql
CREATE TABLE `RSDST` (
  `LANGU` char(2) NOT NULL COMMENT 'LANGU',
  `DATASOURCE` char(30) NOT NULL COMMENT 'DATASOURCE',
  `LOGSYS` char(10) NOT NULL COMMENT 'Source system',
  `OBJVERS` char(1) NOT NULL COMMENT 'Object version',
  `TXTSH` char(20) DEFAULT NULL COMMENT 'Short description',
  `TXTMD` char(40) DEFAULT NULL COMMENT 'Medium description',
  `TXTLG` char(60) DEFAULT NULL COMMENT 'Long description',
  PRIMARY KEY (`LANGU`,`DATASOURCE`,`LOGSYS`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='DataSource - Texts'
```

## `rsksfieldnew`

```sql
CREATE TABLE `rsksfieldnew` (
  `ISOURCE` char(30) NOT NULL COMMENT 'ISOURCE',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `SEGID` char(4) NOT NULL COMMENT 'SEGID',
  `POSIT` char(4) NOT NULL COMMENT 'POSIT',
  `IOBJNM` char(30) DEFAULT NULL COMMENT 'IOBJNM',
  `KEYFLAG` char(1) DEFAULT NULL COMMENT 'KEYFLAG',
  `FIELDNM` char(30) DEFAULT NULL COMMENT 'FIELDNM',
  `DTELNM` char(30) DEFAULT NULL COMMENT 'DTELNM',
  `DATATYPE` char(10) DEFAULT NULL COMMENT 'DATATYPE',
  `INTLEN` char(6) DEFAULT NULL COMMENT 'INTLEN',
  `LENG` char(6) DEFAULT NULL COMMENT 'LENG',
  `INTTYPE` char(1) DEFAULT NULL COMMENT 'INTTYPE',
  `CONVEXIT` char(5) DEFAULT NULL COMMENT 'CONVEXIT',
  `DECIMALS` char(6) DEFAULT NULL COMMENT 'DECIMALS',
  `UNIFIELDNM` char(30) DEFAULT NULL COMMENT 'UNIFIELDNM',
  `INTEGRITY` char(1) DEFAULT NULL COMMENT 'INTEGRITY',
  `ALPHA_CONVERSION` char(1) DEFAULT NULL COMMENT 'ALPHA_CONVERSION',
  `TYPE` char(3) DEFAULT NULL COMMENT 'TYPE',
  PRIMARY KEY (`ISOURCE`,`OBJVERS`,`SEGID`,`POSIT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='InfoSource field metadata'
```

## `rsksfieldnewt`

```sql
CREATE TABLE `rsksfieldnewt` (
  `LANGU` char(2) NOT NULL COMMENT 'LANGU',
  `ISOURCE` char(30) NOT NULL COMMENT 'ISOURCE',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `SEGID` char(4) NOT NULL COMMENT 'SEGID',
  `POSIT` char(4) NOT NULL COMMENT 'POSIT',
  `TXTSH` char(45) DEFAULT NULL COMMENT 'TXTSH',
  `TXTLG` char(60) DEFAULT NULL COMMENT 'TXTLG',
  PRIMARY KEY (`LANGU`,`ISOURCE`,`OBJVERS`,`SEGID`,`POSIT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='InfoSource field text metadata'
```

## `rsksnew`

```sql
CREATE TABLE `rsksnew` (
  `ISOURCE` char(30) NOT NULL COMMENT 'ISOURCE',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `INFOAREA` char(30) DEFAULT NULL COMMENT 'INFOAREA',
  `APPLNM` char(30) DEFAULT NULL COMMENT 'APPLNM',
  `OBJSTAT` char(3) DEFAULT NULL COMMENT 'OBJSTAT',
  `ACTIVFL` char(1) DEFAULT NULL COMMENT 'ACTIVFL',
  `OWNER` char(12) DEFAULT NULL COMMENT 'OWNER',
  `CONTENT` char(6) DEFAULT NULL COMMENT 'CONTENT',
  `TSTPNM` char(12) DEFAULT NULL COMMENT 'TSTPNM',
  `TIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'TIMESTMP',
  `CONTTIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'CONTTIMESTMP',
  `TLOGO_OWNED_BY` char(4) DEFAULT NULL COMMENT 'TLOGO_OWNED_BY',
  `OBJNM_OWNED_BY` char(40) DEFAULT NULL COMMENT 'OBJNM_OWNED_BY',
  `ODSO_LIKE` char(1) NOT NULL COMMENT 'ODSO_LIKE',
  `AGGREGATION` char(1) NOT NULL COMMENT 'AGGREGATION',
  `VERSION` int NOT NULL COMMENT 'VERSION',
  `CREATED_AT` decimal(15,0) DEFAULT NULL COMMENT 'CREATED_AT',
  `CREATED_BY` char(12) DEFAULT NULL COMMENT 'CREATED_BY',
  PRIMARY KEY (`ISOURCE`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='InfoSource master metadata'
```

## `rsksnewt`

```sql
CREATE TABLE `rsksnewt` (
  `LANGU` char(2) NOT NULL COMMENT 'LANGU',
  `ISOURCE` char(30) NOT NULL COMMENT 'ISOURCE',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `TXTLG` char(60) DEFAULT NULL COMMENT 'TXTLG',
  PRIMARY KEY (`LANGU`,`ISOURCE`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='InfoSource text metadata'
```

## `rsoadso`

```sql
CREATE TABLE `rsoadso` (
  `ADSONM` char(30) NOT NULL COMMENT 'ADSONM',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `CONTREL` char(6) DEFAULT NULL COMMENT 'CONTREL',
  `CONTTIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'CONTTIMESTMP',
  `OWNER` char(12) DEFAULT NULL COMMENT 'OWNER',
  `BWAPPL` char(10) DEFAULT NULL COMMENT 'BWAPPL',
  `INFOAREA` char(30) DEFAULT NULL COMMENT 'INFOAREA',
  `TSTPNM` char(12) DEFAULT NULL COMMENT 'TSTPNM',
  `TIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'TIMESTMP',
  `CRNM` char(12) DEFAULT NULL COMMENT 'CRNM',
  `CRTSTP` decimal(15,0) DEFAULT NULL COMMENT 'CRTSTP',
  `ABAP_LANGUAGE_VERSION` char(1) DEFAULT NULL COMMENT 'ABAP_LANGUAGE_VERSION',
  `XML_UI` mediumtext COMMENT 'XML_UI',
  `ACTIVATE_DATA` char(1) DEFAULT NULL COMMENT 'ACTIVATE_DATA',
  `WRITE_CHANGELOG` char(1) DEFAULT NULL COMMENT 'WRITE_CHANGELOG',
  `CUBEDELTAONLY` char(1) DEFAULT NULL COMMENT 'CUBEDELTAONLY',
  `NO_AQ_DELETION` char(1) DEFAULT NULL COMMENT 'NO_AQ_DELETION',
  `UNIQUE_RECORDS` char(1) DEFAULT NULL COMMENT 'UNIQUE_RECORDS',
  `PLANNING_MODE` char(1) DEFAULT NULL COMMENT 'PLANNING_MODE',
  `CHECK_DELTA_CONS` char(1) DEFAULT NULL COMMENT 'CHECK_DELTA_CONS',
  `EXTENDED_AQ_TABLE` char(1) DEFAULT NULL COMMENT 'EXTENDED_AQ_TABLE',
  `NCUMTIM` char(30) DEFAULT NULL COMMENT 'NCUMTIM',
  `ALL_SIDS_CHECKED` char(1) DEFAULT NULL COMMENT 'ALL_SIDS_CHECKED',
  `ALL_SIDS_MATERIALIZED` char(1) DEFAULT NULL COMMENT 'ALL_SIDS_MATERIALIZED',
  `HANAMODELFL` char(1) DEFAULT NULL COMMENT 'HANAMODELFL',
  `DIRECT_UPDATE` char(1) DEFAULT NULL COMMENT 'DIRECT_UPDATE',
  `SNAPSHOT_SCENARIO` char(1) DEFAULT NULL COMMENT 'SNAPSHOT_SCENARIO',
  `DYN_TIERING_PER_PART` char(1) DEFAULT NULL COMMENT 'DYN_TIERING_PER_PART',
  `TEMPERATURE_SCHEMA` char(4) DEFAULT NULL COMMENT 'TEMPERATURE_SCHEMA',
  `IS_REPORTING_OBJ` char(1) DEFAULT NULL COMMENT 'IS_REPORTING_OBJ',
  `FORCE_NO_CONCAT` char(1) DEFAULT NULL COMMENT 'FORCE_NO_CONCAT',
  `INVERTED_INDIVIDUALS` char(1) DEFAULT NULL COMMENT 'INVERTED_INDIVIDUALS',
  `COMPATIBILITY_VIEWS` char(1) DEFAULT NULL COMMENT 'COMPATIBILITY_VIEWS',
  `AUTOREFRESH` char(1) DEFAULT NULL COMMENT 'AUTOREFRESH',
  `PUSHMODE` char(1) DEFAULT NULL COMMENT 'PUSHMODE',
  `EXCEPT_UPDATE` char(1) DEFAULT NULL COMMENT 'EXCEPT_UPDATE',
  `COLD_CONNAME` char(10) DEFAULT NULL COMMENT 'COLD_CONNAME',
  `LOG_RAL_OUTPUT` char(1) DEFAULT NULL COMMENT 'LOG_RAL_OUTPUT',
  `DYNAMIC_PARTITIONS` char(1) DEFAULT NULL COMMENT 'DYNAMIC_PARTITIONS',
  `DYN_PART_GRANULARITY` char(30) DEFAULT NULL COMMENT 'DYN_PART_GRANULARITY',
  `ODATAFLAG` char(1) DEFAULT NULL COMMENT 'ODATAFLAG',
  `MERGE_MODE` char(10) DEFAULT NULL COMMENT 'MERGE_MODE',
  `UOM_IOBJNM` char(30) DEFAULT NULL COMMENT 'UOM_IOBJNM',
  `GROUP_NAME` char(30) DEFAULT NULL COMMENT 'GROUP_NAME',
  `GROUP_FIELDNM` char(30) DEFAULT NULL COMMENT 'GROUP_FIELDNM',
  PRIMARY KEY (`ADSONM`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ADSO technical metadata'
```

## `RSOADSOT`

```sql
CREATE TABLE `RSOADSOT` (
  `LANGU` char(2) NOT NULL COMMENT 'LANGU',
  `ADSONM` char(30) NOT NULL COMMENT 'DataStore Object Name',
  `OBJVERS` char(1) NOT NULL COMMENT 'Object version',
  `TTYP` char(4) NOT NULL COMMENT 'DataStore Object Text Type',
  `COLNAME` char(200) NOT NULL COMMENT 'Datastore Object Column Name',
  `DESCRIPTION` text COMMENT 'Datastore Objects: Description',
  `QUICK_INFO` text COMMENT 'Quick Info for UI Event "Mouse Over"',
  PRIMARY KEY (`LANGU`,`ADSONM`,`OBJVERS`,`TTYP`,`COLNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Texts for Datastore Object'
```

## `RSTRAN`

```sql
CREATE TABLE `RSTRAN` (
  `TRANID` char(32) NOT NULL COMMENT 'Transformation ID',
  `OBJVERS` char(1) NOT NULL COMMENT 'Object version',
  `OBJSTAT` char(3) DEFAULT NULL COMMENT 'Object Status',
  `CONTREL` char(6) DEFAULT NULL COMMENT 'Content release',
  `CONTTIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'Content time stamp: Last modification to the object by SAP',
  `OWNER` char(12) DEFAULT NULL COMMENT 'Owner (Person Responsible)',
  `BWAPPL` char(10) DEFAULT NULL COMMENT 'BW Application (Namespace)',
  `ACTIVFL` char(1) DEFAULT NULL COMMENT 'Active and revised version do not agree',
  `ABAP_LANGUAGE_VERSION` char(1) DEFAULT NULL COMMENT 'ABAP_LANGUAGE_VERSION',
  `TSTPNM` char(12) DEFAULT NULL COMMENT 'Last Changed By',
  `TIMESTMP` decimal(15,0) DEFAULT NULL COMMENT 'UTC Time Stamp in Short Form (YYYYMMDDhhmmss)',
  `SOURCETYPE` char(4) DEFAULT NULL COMMENT 'BW: Object Type (TLOGO)',
  `SOURCESUBTYPE` char(4) DEFAULT NULL COMMENT 'BW Metadata Repository: Subtype of TLOGO Type',
  `SOURCENAME` char(40) DEFAULT NULL COMMENT 'SOURCENAME',
  `SOURCE` varchar(40) DEFAULT NULL COMMENT 'Source object parsed from SOURCENAME before first space',
  `SOURCESYS` varchar(40) DEFAULT NULL COMMENT 'Source system parsed from SOURCENAME after first space',
  `TARGETTYPE` char(4) DEFAULT NULL COMMENT 'BW: Object Type (TLOGO)',
  `TARGETSUBTYPE` char(4) DEFAULT NULL COMMENT 'BW Metadata Repository: Subtype of TLOGO Type',
  `TARGETNAME` char(40) DEFAULT NULL COMMENT 'TARGETNAME',
  `STARTROUTINE` char(25) DEFAULT NULL COMMENT 'BW Generation tool: GUID in compressed form (CHAR25)',
  `ENDROUTINE` char(25) DEFAULT NULL COMMENT 'BW Generation tool: GUID in compressed form (CHAR25)',
  `EXPERT` char(25) DEFAULT NULL COMMENT 'BW Generation tool: GUID in compressed form (CHAR25)',
  `GLBCODE` char(25) DEFAULT NULL COMMENT 'BW Generation tool: GUID in compressed form (CHAR25)',
  `TRANPROG` char(25) DEFAULT NULL COMMENT 'Program ID for Transformation (Generated)',
  `VERSION_CUR` smallint DEFAULT NULL COMMENT 'VERSION_CUR',
  `TARGET_TAB_TYPE` char(1) DEFAULT NULL COMMENT 'Table Type',
  `IS_SHADOW` char(1) NOT NULL COMMENT 'Boolean',
  `SHADOW_TRANID` char(32) NOT NULL COMMENT 'Transformation ID',
  `GLBCODE2` char(25) NOT NULL COMMENT 'BW Generation tool: GUID in compressed form (CHAR25)',
  `TLOGO_OWNED_BY` char(4) DEFAULT NULL COMMENT 'BW: Object Type (TLOGO)',
  `OBJNM_OWNED_BY` char(40) DEFAULT NULL COMMENT 'OBJNM_OWNED_BY',
  `CURRUNIT_ALLOWED` char(1) NOT NULL COMMENT 'Boolean',
  `GROUPING` char(1) DEFAULT NULL COMMENT 'Boolean',
  `ALL_FIELDS` char(1) NOT NULL COMMENT 'Set Update Behavior for Fields of End Routine',
  `ORGTRANID` char(32) DEFAULT NULL COMMENT 'Transformation ID',
  `TUNNEL` char(1) DEFAULT NULL COMMENT 'Boolean',
  `STRUCTURE_TYPE` char(1) NOT NULL COMMENT 'Structure Type',
  `DOUBLE_RECORDS` char(1) NOT NULL COMMENT 'Boolean',
  `UNIT_CHECK` char(1) NOT NULL COMMENT 'Boolean',
  `HAAP_HINT` char(1) NOT NULL COMMENT 'Enable Error Handling',
  `SET_RUNTIME` char(1) NOT NULL COMMENT '3value boolean for realization',
  `CREATED_AT` decimal(15,0) DEFAULT NULL COMMENT 'Creation UTC Time Stamp in Short Form (YYYYMMDDhhmmss)',
  `CREATED_BY` char(12) DEFAULT NULL COMMENT 'Tlogo Object Created by',
  `ERROR_HANDLER_OFF` char(1) NOT NULL COMMENT 'Boolean',
  `INV_ROUT_OFF` char(1) NOT NULL COMMENT 'Boolean',
  `NULL_CHECK_ON` char(1) NOT NULL COMMENT 'Boolean',
  PRIMARY KEY (`TRANID`,`OBJVERS`),
  KEY `idx_sourcename_objvers` (`SOURCENAME`,`OBJVERS`),
  KEY `idx_targetname_objvers` (`TARGETNAME`,`OBJVERS`),
  KEY `idx_source_objvers` (`SOURCE`,`OBJVERS`),
  KEY `idx_sourcesys_objvers` (`SOURCESYS`,`OBJVERS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Transformation'
```

## `rstran_mapping_rule`

```sql
CREATE TABLE `rstran_mapping_rule` (
  `tran_id` varchar(64) NOT NULL,
  `rule_id` int NOT NULL,
  `step_id` int NOT NULL,
  `seg_id` varchar(64) NOT NULL DEFAULT '',
  `pair_index` int NOT NULL,
  `ruleposit` varchar(64) NOT NULL DEFAULT '',
  `source_field` varchar(255) DEFAULT NULL,
  `target_field` varchar(255) DEFAULT NULL,
  `target_keyflag` varchar(8) DEFAULT NULL,
  `rule_type` varchar(32) DEFAULT NULL,
  `aggr` varchar(64) DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tran_id`,`rule_id`,`step_id`,`seg_id`,`pair_index`),
  KEY `idx_mapping_rule_tran` (`tran_id`),
  KEY `idx_mapping_rule_source` (`tran_id`,`source_field`),
  KEY `idx_mapping_rule_target` (`tran_id`,`target_field`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

## `rstran_mapping_rule_full`

```sql
CREATE TABLE `rstran_mapping_rule_full` (
  `tran_id` varchar(64) NOT NULL,
  `rule_id` int NOT NULL,
  `step_id` int NOT NULL,
  `seg_id` varchar(64) NOT NULL DEFAULT '',
  `pair_index` int NOT NULL DEFAULT '0',
  `display_order` int NOT NULL DEFAULT '0',
  `source_object` varchar(255) NOT NULL DEFAULT '',
  `source_system` varchar(64) NOT NULL DEFAULT '',
  `target_object` varchar(255) NOT NULL DEFAULT '',
  `target_system` varchar(64) NOT NULL DEFAULT '',
  `source_type` varchar(32) NOT NULL DEFAULT '',
  `target_type` varchar(32) NOT NULL DEFAULT '',
  `source_field` varchar(255) NOT NULL DEFAULT '',
  `target_field` varchar(255) NOT NULL DEFAULT '',
  `source_field_origin` varchar(32) NOT NULL DEFAULT '',
  `target_field_origin` varchar(32) NOT NULL DEFAULT '',
  `source_field_matched` tinyint(1) NOT NULL DEFAULT '0',
  `target_field_matched` tinyint(1) NOT NULL DEFAULT '0',
  `source_field_is_key` varchar(8) DEFAULT NULL,
  `target_field_is_key` varchar(8) DEFAULT NULL,
  `rule_type` varchar(32) DEFAULT NULL,
  `aggr` varchar(64) DEFAULT NULL,
  `ruleposit` varchar(64) NOT NULL DEFAULT '',
  `row_kind` varchar(24) NOT NULL DEFAULT 'mapped',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tran_id`,`rule_id`,`step_id`,`seg_id`,`pair_index`,`source_field`,`target_field`),
  KEY `idx_mapping_rule_full_tran` (`tran_id`),
  KEY `idx_mapping_rule_full_source_obj` (`source_object`,`source_type`),
  KEY `idx_mapping_rule_full_target_obj` (`target_object`,`target_type`),
  KEY `idx_mapping_rule_full_source_field` (`tran_id`,`source_field`),
  KEY `idx_mapping_rule_full_target_field` (`tran_id`,`target_field`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Materialized full field coverage by transformation step'
```

## `RSTRANFIELD`

```sql
CREATE TABLE `RSTRANFIELD` (
  `TRANID` char(32) NOT NULL COMMENT 'Transformation ID',
  `OBJVERS` char(1) NOT NULL COMMENT 'Object version',
  `SEGID` char(4) NOT NULL COMMENT 'Segment Number',
  `RULEID` smallint NOT NULL COMMENT 'Number of Rule Within a Transformation',
  `STEPID` tinyint unsigned NOT NULL COMMENT 'ID for Rule Step',
  `PARAMTYPE` char(1) NOT NULL COMMENT 'Rule Parameter Type',
  `PARAMNM` char(30) NOT NULL COMMENT 'Field name',
  `FIELDNM` char(30) DEFAULT NULL COMMENT 'Field name',
  `FIELDTYPE` char(1) DEFAULT NULL COMMENT 'Type of Structure Field',
  `KEYFLAG` char(1) DEFAULT NULL COMMENT 'KEYFLAG',
  `RULEPOSIT` char(4) DEFAULT NULL COMMENT 'Position of the Field in the Structure / Table',
  `AGGR` char(3) NOT NULL COMMENT 'Aggregation Behavior in Transformation',
  PRIMARY KEY (`TRANID`,`OBJVERS`,`SEGID`,`RULEID`,`STEPID`,`PARAMTYPE`,`PARAMNM`),
  KEY `idx_tranid_ruleid` (`TRANID`,`RULEID`),
  KEY `idx_tranid_objvers_ruleid_paramtype` (`TRANID`,`OBJVERS`,`RULEID`,`PARAMTYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Mapping of Rule Parameters - Structure Fields'
```

## `RSTRANRULE`

```sql
CREATE TABLE `RSTRANRULE` (
  `TRANID` char(32) NOT NULL COMMENT 'Transformation ID',
  `OBJVERS` char(1) NOT NULL COMMENT 'Object version',
  `RULEID` smallint NOT NULL COMMENT 'Number of Rule Within a Transformation',
  `SEQNR` char(4) DEFAULT NULL COMMENT 'Sequence Number',
  `GROUPID` char(2) DEFAULT NULL COMMENT 'Number of Transformation Group',
  `GROUPTYPE` char(1) DEFAULT NULL COMMENT 'Type of Group',
  `FIELD_USAGE` char(1) DEFAULT NULL COMMENT 'Use of Source Structure Fields',
  `RULETYPE` char(30) DEFAULT NULL COMMENT 'Type of Rule',
  `REF_RULE` smallint DEFAULT NULL COMMENT 'Number of Rule Within a Transformation',
  `AGGR` char(3) DEFAULT NULL COMMENT 'Aggregation Behavior in Transformation',
  `NO_CONV` char(1) NOT NULL COMMENT 'Boolean',
  `AMDP` char(1) DEFAULT NULL COMMENT 'AMDP Execution',
  `ABAP` char(1) DEFAULT NULL COMMENT 'ABAP runtime',
  PRIMARY KEY (`TRANID`,`OBJVERS`,`RULEID`),
  KEY `idx_tranid_ruleid` (`TRANID`,`RULEID`),
  KEY `idx_grouptype` (`GROUPTYPE`),
  KEY `idx_tranid_objvers_ruleid` (`TRANID`,`OBJVERS`,`RULEID`),
  KEY `idx_objvers_groupid_tranid_ruleid` (`OBJVERS`,`GROUPID`,`TRANID`,`RULEID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Transformation Rule'
```

## `rstranstepcnst`

```sql
CREATE TABLE `rstranstepcnst` (
  `TRANID` char(32) NOT NULL COMMENT 'TRANID',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `RULEID` smallint NOT NULL COMMENT 'RULEID',
  `STEPID` tinyint unsigned NOT NULL COMMENT 'STEPID',
  `VALUE` char(255) DEFAULT NULL COMMENT 'VALUE',
  `INTTYPE` char(1) DEFAULT NULL COMMENT 'INTTYPE',
  `LENGTH` int DEFAULT NULL COMMENT 'LENGTH',
  `DECIMALS` tinyint unsigned DEFAULT NULL COMMENT 'DECIMALS',
  PRIMARY KEY (`TRANID`,`OBJVERS`,`RULEID`,`STEPID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Transformation constant rule metadata'
```

## `rstransteprout`

```sql
CREATE TABLE `rstransteprout` (
  `TRANID` char(32) NOT NULL COMMENT 'TRANID',
  `OBJVERS` char(1) NOT NULL COMMENT 'OBJVERS',
  `RULEID` smallint NOT NULL COMMENT 'RULEID',
  `STEPID` tinyint unsigned NOT NULL COMMENT 'STEPID',
  `ON_HANA` char(1) NOT NULL COMMENT 'ON_HANA',
  `CODEID` char(25) DEFAULT NULL COMMENT 'CODEID',
  `KIND` char(7) DEFAULT NULL COMMENT 'KIND',
  `NAME` char(30) DEFAULT NULL COMMENT 'NAME',
  `CODE` mediumtext COMMENT 'CODE',
  `CODE_INV` text COMMENT 'CODE_INV',
  PRIMARY KEY (`TRANID`,`OBJVERS`,`RULEID`,`STEPID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Transformation formula and routine rule metadata'
```

## `user_hidden_object`

```sql
CREATE TABLE `user_hidden_object` (
  `bw_object` varchar(40) NOT NULL COMMENT 'BW Object',
  `sourcesys` varchar(25) NOT NULL DEFAULT '' COMMENT 'Source System',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`bw_object`,`sourcesys`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户隐藏对象清单'
```

## `user_sessions`

```sql
CREATE TABLE `user_sessions` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `username` varchar(64) NOT NULL,
  `session_hash` char(64) NOT NULL,
  `expires_at` datetime NOT NULL,
  `revoked` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_session_hash` (`session_hash`),
  KEY `idx_session_user` (`username`),
  CONSTRAINT `fk_sessions_user` FOREIGN KEY (`username`) REFERENCES `users` (`username`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=89 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

## `users`

```sql
CREATE TABLE `users` (
  `username` varchar(64) NOT NULL,
  `password_hash` varchar(255) NOT NULL,
  `role` varchar(16) NOT NULL DEFAULT 'user',
  `is_locked` tinyint(1) NOT NULL DEFAULT '0',
  `failed_attempts` int NOT NULL DEFAULT '0',
  `temp_lock_until` datetime DEFAULT NULL,
  `last_login_at` datetime DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```
