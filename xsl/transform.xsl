<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml"/>
    <xsl:param name="mainClassName"/>

    <xsl:template match="/">
        <xsl:call-template name="root"/>
    </xsl:template>

    <xsl:template name="root">
        <xsl:value-of select="*"/>
    </xsl:template>

</xsl:stylesheet>