package com.amadeus.sparklear.translators

import com.amadeus.sparklear.translators.SqlTranslator.EntityNameSql
import com.amadeus.testfwk.SimpleSpec

class TranslatorSpec extends SimpleSpec {
  describe(s"The ${Translator.getClass.getName}") {
    it("should resolve translators by name") {
      val t = Translator.forName(SqlTranslator.Translators)(EntityNameSql, "SQLplannode")
      t.name should be("sqlplannode")

      val u = Translator.forName(SqlTranslator.Translators)(EntityNameSql, "sqlprettY")
      u.name should be("sqlpretty")
    }

    it("should fail gracefully when invalid translators name is provided") {
      val e = intercept[IllegalArgumentException](Translator.forName(SqlTranslator.Translators)(EntityNameSql, "unknown"))
      e.getMessage should be("Invalid translator 'unknown' for entity SQL (expected one of: sqlplannode, sqlpretty)")
    }
  }

}
