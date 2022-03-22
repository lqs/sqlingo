package generator

import "testing"

func TestConvert(t *testing.T) {
	m := map[string]string{
		"abc":             "Abc",
		"name":            "Name",
		"Name":            "Name",
		"abc_def":         "AbcDef",
		"ϢϢϢϢ1":           "ϢϢϢϢ1",
		"_[[[[[":          "E",
		"abc/def":         "AbcDef",
		"中文开头":            "E中文开头",
		"abc~~中文":         "Abc中文",
		"abc-def--ghi":    "AbcDefGhi",
		"user_id":         "UserID",
		"user_ip_address": "UserIPAddress",
		"user_ix":         "UserIx",
		"volume_db":       "VolumedB",
		"db_volume":       "EdBVolume",
	}
	for k, v := range m {
		if convertToExportedIdentifier(k, []string{"ID", "IP", "dB"}) != v {
			t.Errorf("'%s' should be converted to '%s'", k, v)
		}
	}
}
