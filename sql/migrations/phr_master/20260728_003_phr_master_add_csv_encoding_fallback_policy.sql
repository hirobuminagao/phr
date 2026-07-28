ALTER TABLE `phr_master`.`csv_format_versions`
  ADD COLUMN `encoding_fallback_policy` varchar(32) NOT NULL DEFAULT 'ALLOW_COMMON_ENCODINGS'
    AFTER `character_encoding`;
