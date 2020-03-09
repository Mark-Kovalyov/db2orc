<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />

  <xsl:param name="mainClassName"/>

    <xsl:template match="@*|node()" name="Identity">
        <xsl:copy>
            <xsl:choose>
              <xsl:when test="name() = 'Main-Class'">
                <xsl:value-of select="$mainClassName"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:apply-templates select="@*|node()"/>
              </xsl:otherwise>
            </xsl:choose>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
