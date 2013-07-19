<!-- An XSL style sheet for standardizing presentation MathML Expressions 
	$Id$ $HeadURL$ Copyright (c) 2012 Michael Kohlhase, -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:m="http://www.w3.org/1998/Math/MathML" xmlns="http://www.w3.org/1998/Math/MathML"
	xmlns:str="http://exslt.org/strings" extension-element-prefixes="str"
	exclude-result-prefixes="m str" version="1.0">
	<xsl:output method="xml" version="1.0" encoding="UTF-8"
		indent="yes" />
	<!-- default behavior: copy -->
	<xsl:template match="*">
		<!-- <xsl:message>copy <xsl:value-of select="local-name()"/></xsl:message> -->
		<xsl:copy>
			<xsl:copy-of select="@*" />
			<xsl:apply-templates />
		</xsl:copy>
	</xsl:template>
	<!-- implied mrows (we delete them) -->
	<xsl:template match="math[mrow]">
		<math>
			 <xsl:apply-templates select="mrow/*" />
		</math>
	</xsl:template>
	<!-- mfenced is compiled into mrow and mo -->
	<xsl:template match="mfenced">
		<mrow>
			<mo fence="true">
				<xsl:choose>
					<xsl:when test="@open">
						<xsl:value-of select="@open" />
					</xsl:when>
					<xsl:otherwise>
						<xsl:text>(</xsl:text>
					</xsl:otherwise>
				</xsl:choose>
			</mo>
			<xsl:choose>
				<xsl:when test="@separators">
					<xsl:variable name="separators"
						select="str:tokenize(translate(@separators,' ',''),'')" />
					<xsl:variable name="sepnums" select="count($separators)" />
					<xsl:for-each select="*">
						<xsl:apply-templates select="." />
						<xsl:choose>
							<xsl:when test="position()=last()" />
							<xsl:when test="position()&lt; $sepnums">
								<mo separator="true">
									<xsl:value-of select="$separators[position()]" />
								</mo>
							</xsl:when>
							<xsl:otherwise>
								<mo separator="true">
									<xsl:value-of select="$separators[$sepnums]" />
								</mo>
							</xsl:otherwise>
						</xsl:choose>
					</xsl:for-each>
				</xsl:when>
				<xsl:otherwise>
					<xsl:for-each select="*">
						<xsl:apply-templates select="." />
						<xsl:if test="position()!=last()">
							<mo separator="true">,</mo>
						</xsl:if>
					</xsl:for-each>
				</xsl:otherwise>
			</xsl:choose>
			<mo fence="true">
				<xsl:choose>
					<xsl:when test="@close">
						<xsl:value-of select="@close" />
					</xsl:when>
					<xsl:otherwise>
						<xsl:text>)</xsl:text>
					</xsl:otherwise>
				</xsl:choose>
			</mo>
		</mrow>
	</xsl:template>
		<!-- mi mathvariant is compiled into the corresponding unicode char-->
	<xsl:template match="mi[@mathvariant]">
					<mo>
				<xsl:choose>
<xsl:when test="@mathvariant='bold-italic'">
	<xsl:if test=".='ο'"><xsl:text>&#120644;</xsl:text></xsl:if>
	<xsl:if test=".='S'"><xsl:text>&#119930;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#119922;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#119941;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#119936;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#119916;</xsl:text></xsl:if>
	<xsl:if test=".='Τ'"><xsl:text>&#120623;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#119962;</xsl:text></xsl:if>
	<xsl:if test=".='Π'"><xsl:text>&#120619;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#119944;</xsl:text></xsl:if>
	<xsl:if test=".='δ'"><xsl:text>&#120633;</xsl:text></xsl:if>
	<xsl:if test=".='Κ'"><xsl:text>&#120613;</xsl:text></xsl:if>
	<xsl:if test=".='ω'"><xsl:text>&#120654;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#119942;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#119921;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#119954;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#119939;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#119915;</xsl:text></xsl:if>
	<xsl:if test=".='Ω'"><xsl:text>&#120628;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#119963;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#119960;</xsl:text></xsl:if>
	<xsl:if test=".='΢'"><xsl:text>&#120621;</xsl:text></xsl:if>
	<xsl:if test=".='Θ'"><xsl:text>&#120611;</xsl:text></xsl:if>
	<xsl:if test=".='Ι'"><xsl:text>&#120612;</xsl:text></xsl:if>
	<xsl:if test=".='Ν'"><xsl:text>&#120616;</xsl:text></xsl:if>
	<xsl:if test=".='Ε'"><xsl:text>&#120608;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#119928;</xsl:text></xsl:if>
	<xsl:if test=".='ς'"><xsl:text>&#120647;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#119924;</xsl:text></xsl:if>
	<xsl:if test=".='Ψ'"><xsl:text>&#120627;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#119914;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#119923;</xsl:text></xsl:if>
	<xsl:if test=".='σ'"><xsl:text>&#120648;</xsl:text></xsl:if>
	<xsl:if test=".='ζ'"><xsl:text>&#120635;</xsl:text></xsl:if>
	<xsl:if test=".='θ'"><xsl:text>&#120637;</xsl:text></xsl:if>
	<xsl:if test=".='Ο'"><xsl:text>&#120618;</xsl:text></xsl:if>
	<xsl:if test=".='Γ'"><xsl:text>&#120606;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#119935;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#119927;</xsl:text></xsl:if>
	<xsl:if test=".='ι'"><xsl:text>&#120638;</xsl:text></xsl:if>
	<xsl:if test=".='Ρ'"><xsl:text>&#120620;</xsl:text></xsl:if>
	<xsl:if test=".='ε'"><xsl:text>&#120634;</xsl:text></xsl:if>
	<xsl:if test=".='Δ'"><xsl:text>&#120607;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#119931;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#119938;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#119925;</xsl:text></xsl:if>
	<xsl:if test=".='ρ'"><xsl:text>&#120646;</xsl:text></xsl:if>
	<xsl:if test=".='Φ'"><xsl:text>&#120625;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#119947;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#119937;</xsl:text></xsl:if>
	<xsl:if test=".='Σ'"><xsl:text>&#120622;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#119958;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#119948;</xsl:text></xsl:if>
	<xsl:if test=".='Α'"><xsl:text>&#120604;</xsl:text></xsl:if>
	<xsl:if test=".='Η'"><xsl:text>&#120610;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#119957;</xsl:text></xsl:if>
	<xsl:if test=".='λ'"><xsl:text>&#120640;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#119934;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#119959;</xsl:text></xsl:if>
	<xsl:if test=".='τ'"><xsl:text>&#120649;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#119913;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#119956;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#119919;</xsl:text></xsl:if>
	<xsl:if test=".='ν'"><xsl:text>&#120642;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#119940;</xsl:text></xsl:if>
	<xsl:if test=".='ξ'"><xsl:text>&#120643;</xsl:text></xsl:if>
	<xsl:if test=".='β'"><xsl:text>&#120631;</xsl:text></xsl:if>
	<xsl:if test=".='Μ'"><xsl:text>&#120615;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#119920;</xsl:text></xsl:if>
	<xsl:if test=".='Λ'"><xsl:text>&#120614;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#119918;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#119932;</xsl:text></xsl:if>
	<xsl:if test=".='γ'"><xsl:text>&#120632;</xsl:text></xsl:if>
	<xsl:if test=".='α'"><xsl:text>&#120630;</xsl:text></xsl:if>
	<xsl:if test=".='Υ'"><xsl:text>&#120624;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#119917;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#119955;</xsl:text></xsl:if>
	<xsl:if test=".='Χ'"><xsl:text>&#120626;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#119961;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#119933;</xsl:text></xsl:if>
	<xsl:if test=".='Ξ'"><xsl:text>&#120617;</xsl:text></xsl:if>
	<xsl:if test=".='μ'"><xsl:text>&#120641;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#119945;</xsl:text></xsl:if>
	<xsl:if test=".='φ'"><xsl:text>&#120651;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#119943;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#119946;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#119912;</xsl:text></xsl:if>
	<xsl:if test=".='Β'"><xsl:text>&#120605;</xsl:text></xsl:if>
	<xsl:if test=".='π'"><xsl:text>&#120645;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#119926;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#119951;</xsl:text></xsl:if>
	<xsl:if test=".='υ'"><xsl:text>&#120650;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#119950;</xsl:text></xsl:if>
	<xsl:if test=".='χ'"><xsl:text>&#120652;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#119949;</xsl:text></xsl:if>
	<xsl:if test=".='κ'"><xsl:text>&#120639;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#119953;</xsl:text></xsl:if>
	<xsl:if test=".='ψ'"><xsl:text>&#120653;</xsl:text></xsl:if>
	<xsl:if test=".='η'"><xsl:text>&#120636;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#119929;</xsl:text></xsl:if>
	<xsl:if test=".='Ζ'"><xsl:text>&#120609;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#119952;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='script'">
	<xsl:if test=".='S'"><xsl:text>&#119982;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#119990;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#119983;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#119977;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#119974;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#119993;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#119988;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#8496;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#119999;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120014;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#119989;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120010;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120000;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#8458;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120009;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#8495;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#119973;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#119986;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120011;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120008;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#8492;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#8459;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#119992;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120006;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#119991;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#119967;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#8464;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#119970;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120015;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#119984;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120012;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#8497;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120007;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120013;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#119985;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#119980;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#119997;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#8499;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#119966;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#8466;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#119995;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#119998;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#119964;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120003;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#119978;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#119987;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#119979;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120002;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120001;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120005;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#8475;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#8500;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='monospace'">
	<xsl:if test=".='S'"><xsl:text>&#120450;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120458;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120451;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120445;</xsl:text></xsl:if>
	<xsl:if test=".='7'"><xsl:text>&#120829;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120442;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120461;</xsl:text></xsl:if>
	<xsl:if test=".='2'"><xsl:text>&#120824;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120456;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120436;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120467;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120482;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120457;</xsl:text></xsl:if>
	<xsl:if test=".='1'"><xsl:text>&#120823;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120478;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120468;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120464;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120477;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120462;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120441;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120454;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120479;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120476;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120433;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120439;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120460;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120474;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120459;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120435;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120440;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120438;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120483;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120452;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120480;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120437;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120475;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120481;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120453;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120448;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120465;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120444;</xsl:text></xsl:if>
	<xsl:if test=".='0'"><xsl:text>&#120822;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120434;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120443;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120463;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120466;</xsl:text></xsl:if>
	<xsl:if test=".='6'"><xsl:text>&#120828;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120432;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120471;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120446;</xsl:text></xsl:if>
	<xsl:if test=".='3'"><xsl:text>&#120825;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120455;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120447;</xsl:text></xsl:if>
	<xsl:if test=".='9'"><xsl:text>&#120831;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120470;</xsl:text></xsl:if>
	<xsl:if test=".='8'"><xsl:text>&#120830;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120469;</xsl:text></xsl:if>
	<xsl:if test=".='4'"><xsl:text>&#120826;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120473;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120449;</xsl:text></xsl:if>
	<xsl:if test=".='5'"><xsl:text>&#120827;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120472;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='double-struck'">
	<xsl:if test=".='S'"><xsl:text>&#120138;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120146;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120139;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#8469;</xsl:text></xsl:if>
	<xsl:if test=".='7'"><xsl:text>&#120799;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120130;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120149;</xsl:text></xsl:if>
	<xsl:if test=".='2'"><xsl:text>&#120794;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120144;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120124;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120155;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120170;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#8484;</xsl:text></xsl:if>
	<xsl:if test=".='1'"><xsl:text>&#120793;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120166;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120156;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120152;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120165;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120150;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120129;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120142;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120167;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120164;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120121;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#8461;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120148;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120162;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120147;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120123;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120128;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120126;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120171;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120140;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120168;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120125;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120163;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120169;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120141;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#8474;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120153;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120132;</xsl:text></xsl:if>
	<xsl:if test=".='0'"><xsl:text>&#120792;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#8450;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120131;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120151;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120154;</xsl:text></xsl:if>
	<xsl:if test=".='6'"><xsl:text>&#120798;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120120;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120159;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120134;</xsl:text></xsl:if>
	<xsl:if test=".='3'"><xsl:text>&#120795;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120143;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#8473;</xsl:text></xsl:if>
	<xsl:if test=".='9'"><xsl:text>&#120801;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120158;</xsl:text></xsl:if>
	<xsl:if test=".='8'"><xsl:text>&#120800;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120157;</xsl:text></xsl:if>
	<xsl:if test=".='4'"><xsl:text>&#120796;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120161;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#8477;</xsl:text></xsl:if>
	<xsl:if test=".='5'"><xsl:text>&#120797;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120160;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='fraktur'">
	<xsl:if test=".='S'"><xsl:text>&#120086;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120094;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120087;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120081;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120078;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120097;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120092;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120072;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120103;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120118;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#8488;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120114;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120104;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120100;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120113;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120098;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120077;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120090;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120115;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120112;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120069;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#8460;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120096;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120110;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120095;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120071;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#8465;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120074;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120119;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120088;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120116;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120073;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120111;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120117;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120089;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120084;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120101;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120080;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#8493;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120079;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120099;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120102;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120068;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120107;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120082;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120091;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120083;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120106;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120105;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120109;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#8476;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120108;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='sans-serif-bold-italic'">
	<xsl:if test=".='ο'"><xsl:text>&#120760;</xsl:text></xsl:if>
	<xsl:if test=".='S'"><xsl:text>&#120398;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120390;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120409;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120404;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120384;</xsl:text></xsl:if>
	<xsl:if test=".='Τ'"><xsl:text>&#120739;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120430;</xsl:text></xsl:if>
	<xsl:if test=".='Π'"><xsl:text>&#120735;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120412;</xsl:text></xsl:if>
	<xsl:if test=".='δ'"><xsl:text>&#120749;</xsl:text></xsl:if>
	<xsl:if test=".='Κ'"><xsl:text>&#120729;</xsl:text></xsl:if>
	<xsl:if test=".='ω'"><xsl:text>&#120770;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120410;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120389;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120422;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120407;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120383;</xsl:text></xsl:if>
	<xsl:if test=".='Ω'"><xsl:text>&#120744;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120431;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120428;</xsl:text></xsl:if>
	<xsl:if test=".='΢'"><xsl:text>&#120737;</xsl:text></xsl:if>
	<xsl:if test=".='Θ'"><xsl:text>&#120727;</xsl:text></xsl:if>
	<xsl:if test=".='Ι'"><xsl:text>&#120728;</xsl:text></xsl:if>
	<xsl:if test=".='Ν'"><xsl:text>&#120732;</xsl:text></xsl:if>
	<xsl:if test=".='Ε'"><xsl:text>&#120724;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120396;</xsl:text></xsl:if>
	<xsl:if test=".='ς'"><xsl:text>&#120763;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120392;</xsl:text></xsl:if>
	<xsl:if test=".='Ψ'"><xsl:text>&#120743;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120382;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120391;</xsl:text></xsl:if>
	<xsl:if test=".='σ'"><xsl:text>&#120764;</xsl:text></xsl:if>
	<xsl:if test=".='ζ'"><xsl:text>&#120751;</xsl:text></xsl:if>
	<xsl:if test=".='θ'"><xsl:text>&#120753;</xsl:text></xsl:if>
	<xsl:if test=".='Ο'"><xsl:text>&#120734;</xsl:text></xsl:if>
	<xsl:if test=".='Γ'"><xsl:text>&#120722;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120403;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120395;</xsl:text></xsl:if>
	<xsl:if test=".='ι'"><xsl:text>&#120754;</xsl:text></xsl:if>
	<xsl:if test=".='Ρ'"><xsl:text>&#120736;</xsl:text></xsl:if>
	<xsl:if test=".='ε'"><xsl:text>&#120750;</xsl:text></xsl:if>
	<xsl:if test=".='Δ'"><xsl:text>&#120723;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120399;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120406;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120393;</xsl:text></xsl:if>
	<xsl:if test=".='ρ'"><xsl:text>&#120762;</xsl:text></xsl:if>
	<xsl:if test=".='Φ'"><xsl:text>&#120741;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120415;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120405;</xsl:text></xsl:if>
	<xsl:if test=".='Σ'"><xsl:text>&#120738;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120426;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120416;</xsl:text></xsl:if>
	<xsl:if test=".='Α'"><xsl:text>&#120720;</xsl:text></xsl:if>
	<xsl:if test=".='Η'"><xsl:text>&#120726;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120425;</xsl:text></xsl:if>
	<xsl:if test=".='λ'"><xsl:text>&#120756;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120402;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120427;</xsl:text></xsl:if>
	<xsl:if test=".='τ'"><xsl:text>&#120765;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120381;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120424;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120387;</xsl:text></xsl:if>
	<xsl:if test=".='ν'"><xsl:text>&#120758;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120408;</xsl:text></xsl:if>
	<xsl:if test=".='ξ'"><xsl:text>&#120759;</xsl:text></xsl:if>
	<xsl:if test=".='β'"><xsl:text>&#120747;</xsl:text></xsl:if>
	<xsl:if test=".='Μ'"><xsl:text>&#120731;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120388;</xsl:text></xsl:if>
	<xsl:if test=".='Λ'"><xsl:text>&#120730;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120386;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120400;</xsl:text></xsl:if>
	<xsl:if test=".='γ'"><xsl:text>&#120748;</xsl:text></xsl:if>
	<xsl:if test=".='α'"><xsl:text>&#120746;</xsl:text></xsl:if>
	<xsl:if test=".='Υ'"><xsl:text>&#120740;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120385;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120423;</xsl:text></xsl:if>
	<xsl:if test=".='Χ'"><xsl:text>&#120742;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120429;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120401;</xsl:text></xsl:if>
	<xsl:if test=".='Ξ'"><xsl:text>&#120733;</xsl:text></xsl:if>
	<xsl:if test=".='μ'"><xsl:text>&#120757;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120413;</xsl:text></xsl:if>
	<xsl:if test=".='φ'"><xsl:text>&#120767;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120411;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120414;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120380;</xsl:text></xsl:if>
	<xsl:if test=".='Β'"><xsl:text>&#120721;</xsl:text></xsl:if>
	<xsl:if test=".='π'"><xsl:text>&#120761;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120394;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120419;</xsl:text></xsl:if>
	<xsl:if test=".='υ'"><xsl:text>&#120766;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120418;</xsl:text></xsl:if>
	<xsl:if test=".='χ'"><xsl:text>&#120768;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120417;</xsl:text></xsl:if>
	<xsl:if test=".='κ'"><xsl:text>&#120755;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120421;</xsl:text></xsl:if>
	<xsl:if test=".='ψ'"><xsl:text>&#120769;</xsl:text></xsl:if>
	<xsl:if test=".='η'"><xsl:text>&#120752;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120397;</xsl:text></xsl:if>
	<xsl:if test=".='Ζ'"><xsl:text>&#120725;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120420;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='sans-serif-italic'">
	<xsl:if test=".='S'"><xsl:text>&#120346;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120354;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120347;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120341;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120338;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120357;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120352;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120332;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120363;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120378;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120353;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120374;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120364;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120360;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120373;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120358;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120337;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120350;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120375;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120372;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120329;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120335;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120356;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120370;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120355;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120331;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120336;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120334;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120379;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120348;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120376;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120333;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120371;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120377;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120349;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120344;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120361;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120340;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120330;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120339;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120359;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120362;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120328;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120367;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120342;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120351;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120343;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120366;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120365;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120369;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120345;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120368;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='bold'">
	<xsl:if test=".='ο'"><xsl:text>&#120528;</xsl:text></xsl:if>
	<xsl:if test=".='S'"><xsl:text>&#119826;</xsl:text></xsl:if>
	<xsl:if test=".='7'"><xsl:text>&#120789;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#119818;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#119837;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#119832;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#119812;</xsl:text></xsl:if>
	<xsl:if test=".='Τ'"><xsl:text>&#120507;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#119858;</xsl:text></xsl:if>
	<xsl:if test=".='Π'"><xsl:text>&#120503;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#119840;</xsl:text></xsl:if>
	<xsl:if test=".='δ'"><xsl:text>&#120517;</xsl:text></xsl:if>
	<xsl:if test=".='Κ'"><xsl:text>&#120497;</xsl:text></xsl:if>
	<xsl:if test=".='ω'"><xsl:text>&#120538;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#119838;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#119817;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#119850;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#119835;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#119811;</xsl:text></xsl:if>
	<xsl:if test=".='Ω'"><xsl:text>&#120512;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#119859;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#119856;</xsl:text></xsl:if>
	<xsl:if test=".='΢'"><xsl:text>&#120505;</xsl:text></xsl:if>
	<xsl:if test=".='Θ'"><xsl:text>&#120495;</xsl:text></xsl:if>
	<xsl:if test=".='Ι'"><xsl:text>&#120496;</xsl:text></xsl:if>
	<xsl:if test=".='Ν'"><xsl:text>&#120500;</xsl:text></xsl:if>
	<xsl:if test=".='Ε'"><xsl:text>&#120492;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#119824;</xsl:text></xsl:if>
	<xsl:if test=".='ς'"><xsl:text>&#120531;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#119820;</xsl:text></xsl:if>
	<xsl:if test=".='Ψ'"><xsl:text>&#120511;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#119810;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#119819;</xsl:text></xsl:if>
	<xsl:if test=".='σ'"><xsl:text>&#120532;</xsl:text></xsl:if>
	<xsl:if test=".='ζ'"><xsl:text>&#120519;</xsl:text></xsl:if>
	<xsl:if test=".='θ'"><xsl:text>&#120521;</xsl:text></xsl:if>
	<xsl:if test=".='Ο'"><xsl:text>&#120502;</xsl:text></xsl:if>
	<xsl:if test=".='Γ'"><xsl:text>&#120490;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#119831;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#119823;</xsl:text></xsl:if>
	<xsl:if test=".='ι'"><xsl:text>&#120522;</xsl:text></xsl:if>
	<xsl:if test=".='Ρ'"><xsl:text>&#120504;</xsl:text></xsl:if>
	<xsl:if test=".='ε'"><xsl:text>&#120518;</xsl:text></xsl:if>
	<xsl:if test=".='Δ'"><xsl:text>&#120491;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#119827;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#119834;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#119821;</xsl:text></xsl:if>
	<xsl:if test=".='ρ'"><xsl:text>&#120530;</xsl:text></xsl:if>
	<xsl:if test=".='2'"><xsl:text>&#120784;</xsl:text></xsl:if>
	<xsl:if test=".='Φ'"><xsl:text>&#120509;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#119843;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#119833;</xsl:text></xsl:if>
	<xsl:if test=".='1'"><xsl:text>&#120783;</xsl:text></xsl:if>
	<xsl:if test=".='Σ'"><xsl:text>&#120506;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#119854;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#119844;</xsl:text></xsl:if>
	<xsl:if test=".='Α'"><xsl:text>&#120488;</xsl:text></xsl:if>
	<xsl:if test=".='Η'"><xsl:text>&#120494;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#119853;</xsl:text></xsl:if>
	<xsl:if test=".='λ'"><xsl:text>&#120524;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#119830;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#119855;</xsl:text></xsl:if>
	<xsl:if test=".='τ'"><xsl:text>&#120533;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#119809;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#119852;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#119815;</xsl:text></xsl:if>
	<xsl:if test=".='ν'"><xsl:text>&#120526;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#119836;</xsl:text></xsl:if>
	<xsl:if test=".='ξ'"><xsl:text>&#120527;</xsl:text></xsl:if>
	<xsl:if test=".='β'"><xsl:text>&#120515;</xsl:text></xsl:if>
	<xsl:if test=".='Μ'"><xsl:text>&#120499;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#119816;</xsl:text></xsl:if>
	<xsl:if test=".='Λ'"><xsl:text>&#120498;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#119814;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#119828;</xsl:text></xsl:if>
	<xsl:if test=".='γ'"><xsl:text>&#120516;</xsl:text></xsl:if>
	<xsl:if test=".='α'"><xsl:text>&#120514;</xsl:text></xsl:if>
	<xsl:if test=".='Υ'"><xsl:text>&#120508;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#119813;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#119851;</xsl:text></xsl:if>
	<xsl:if test=".='Χ'"><xsl:text>&#120510;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#119857;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#119829;</xsl:text></xsl:if>
	<xsl:if test=".='Ξ'"><xsl:text>&#120501;</xsl:text></xsl:if>
	<xsl:if test=".='μ'"><xsl:text>&#120525;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#119841;</xsl:text></xsl:if>
	<xsl:if test=".='0'"><xsl:text>&#120782;</xsl:text></xsl:if>
	<xsl:if test=".='φ'"><xsl:text>&#120535;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#119839;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#119842;</xsl:text></xsl:if>
	<xsl:if test=".='6'"><xsl:text>&#120788;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#119808;</xsl:text></xsl:if>
	<xsl:if test=".='Β'"><xsl:text>&#120489;</xsl:text></xsl:if>
	<xsl:if test=".='π'"><xsl:text>&#120529;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#119822;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#119847;</xsl:text></xsl:if>
	<xsl:if test=".='3'"><xsl:text>&#120785;</xsl:text></xsl:if>
	<xsl:if test=".='υ'"><xsl:text>&#120534;</xsl:text></xsl:if>
	<xsl:if test=".='9'"><xsl:text>&#120791;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#119846;</xsl:text></xsl:if>
	<xsl:if test=".='8'"><xsl:text>&#120790;</xsl:text></xsl:if>
	<xsl:if test=".='χ'"><xsl:text>&#120536;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#119845;</xsl:text></xsl:if>
	<xsl:if test=".='4'"><xsl:text>&#120786;</xsl:text></xsl:if>
	<xsl:if test=".='κ'"><xsl:text>&#120523;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#119849;</xsl:text></xsl:if>
	<xsl:if test=".='ψ'"><xsl:text>&#120537;</xsl:text></xsl:if>
	<xsl:if test=".='η'"><xsl:text>&#120520;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#119825;</xsl:text></xsl:if>
	<xsl:if test=".='Ζ'"><xsl:text>&#120493;</xsl:text></xsl:if>
	<xsl:if test=".='5'"><xsl:text>&#120787;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#119848;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='bold-sans-serif'">
	<xsl:if test=".='ο'"><xsl:text>&#120702;</xsl:text></xsl:if>
	<xsl:if test=".='S'"><xsl:text>&#120294;</xsl:text></xsl:if>
	<xsl:if test=".='7'"><xsl:text>&#120819;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120286;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120305;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120300;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120280;</xsl:text></xsl:if>
	<xsl:if test=".='Τ'"><xsl:text>&#120681;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120326;</xsl:text></xsl:if>
	<xsl:if test=".='Π'"><xsl:text>&#120677;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120308;</xsl:text></xsl:if>
	<xsl:if test=".='δ'"><xsl:text>&#120691;</xsl:text></xsl:if>
	<xsl:if test=".='Κ'"><xsl:text>&#120671;</xsl:text></xsl:if>
	<xsl:if test=".='ω'"><xsl:text>&#120712;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120306;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120285;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120318;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120303;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120279;</xsl:text></xsl:if>
	<xsl:if test=".='Ω'"><xsl:text>&#120686;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120327;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120324;</xsl:text></xsl:if>
	<xsl:if test=".='΢'"><xsl:text>&#120679;</xsl:text></xsl:if>
	<xsl:if test=".='Θ'"><xsl:text>&#120669;</xsl:text></xsl:if>
	<xsl:if test=".='Ι'"><xsl:text>&#120670;</xsl:text></xsl:if>
	<xsl:if test=".='Ν'"><xsl:text>&#120674;</xsl:text></xsl:if>
	<xsl:if test=".='Ε'"><xsl:text>&#120666;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120292;</xsl:text></xsl:if>
	<xsl:if test=".='ς'"><xsl:text>&#120705;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120288;</xsl:text></xsl:if>
	<xsl:if test=".='Ψ'"><xsl:text>&#120685;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120278;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120287;</xsl:text></xsl:if>
	<xsl:if test=".='σ'"><xsl:text>&#120706;</xsl:text></xsl:if>
	<xsl:if test=".='ζ'"><xsl:text>&#120693;</xsl:text></xsl:if>
	<xsl:if test=".='θ'"><xsl:text>&#120695;</xsl:text></xsl:if>
	<xsl:if test=".='Ο'"><xsl:text>&#120676;</xsl:text></xsl:if>
	<xsl:if test=".='Γ'"><xsl:text>&#120664;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120299;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120291;</xsl:text></xsl:if>
	<xsl:if test=".='ι'"><xsl:text>&#120696;</xsl:text></xsl:if>
	<xsl:if test=".='Ρ'"><xsl:text>&#120678;</xsl:text></xsl:if>
	<xsl:if test=".='ε'"><xsl:text>&#120692;</xsl:text></xsl:if>
	<xsl:if test=".='Δ'"><xsl:text>&#120665;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120295;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120302;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120289;</xsl:text></xsl:if>
	<xsl:if test=".='ρ'"><xsl:text>&#120704;</xsl:text></xsl:if>
	<xsl:if test=".='2'"><xsl:text>&#120814;</xsl:text></xsl:if>
	<xsl:if test=".='Φ'"><xsl:text>&#120683;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120311;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120301;</xsl:text></xsl:if>
	<xsl:if test=".='1'"><xsl:text>&#120813;</xsl:text></xsl:if>
	<xsl:if test=".='Σ'"><xsl:text>&#120680;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120322;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120312;</xsl:text></xsl:if>
	<xsl:if test=".='Α'"><xsl:text>&#120662;</xsl:text></xsl:if>
	<xsl:if test=".='Η'"><xsl:text>&#120668;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120321;</xsl:text></xsl:if>
	<xsl:if test=".='λ'"><xsl:text>&#120698;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120298;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120323;</xsl:text></xsl:if>
	<xsl:if test=".='τ'"><xsl:text>&#120707;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120277;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120320;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120283;</xsl:text></xsl:if>
	<xsl:if test=".='ν'"><xsl:text>&#120700;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120304;</xsl:text></xsl:if>
	<xsl:if test=".='ξ'"><xsl:text>&#120701;</xsl:text></xsl:if>
	<xsl:if test=".='β'"><xsl:text>&#120689;</xsl:text></xsl:if>
	<xsl:if test=".='Μ'"><xsl:text>&#120673;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120284;</xsl:text></xsl:if>
	<xsl:if test=".='Λ'"><xsl:text>&#120672;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120282;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120296;</xsl:text></xsl:if>
	<xsl:if test=".='γ'"><xsl:text>&#120690;</xsl:text></xsl:if>
	<xsl:if test=".='α'"><xsl:text>&#120688;</xsl:text></xsl:if>
	<xsl:if test=".='Υ'"><xsl:text>&#120682;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120281;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120319;</xsl:text></xsl:if>
	<xsl:if test=".='Χ'"><xsl:text>&#120684;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120325;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120297;</xsl:text></xsl:if>
	<xsl:if test=".='Ξ'"><xsl:text>&#120675;</xsl:text></xsl:if>
	<xsl:if test=".='μ'"><xsl:text>&#120699;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120309;</xsl:text></xsl:if>
	<xsl:if test=".='0'"><xsl:text>&#120812;</xsl:text></xsl:if>
	<xsl:if test=".='φ'"><xsl:text>&#120709;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120307;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120310;</xsl:text></xsl:if>
	<xsl:if test=".='6'"><xsl:text>&#120818;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120276;</xsl:text></xsl:if>
	<xsl:if test=".='Β'"><xsl:text>&#120663;</xsl:text></xsl:if>
	<xsl:if test=".='π'"><xsl:text>&#120703;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120290;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120315;</xsl:text></xsl:if>
	<xsl:if test=".='3'"><xsl:text>&#120815;</xsl:text></xsl:if>
	<xsl:if test=".='υ'"><xsl:text>&#120708;</xsl:text></xsl:if>
	<xsl:if test=".='9'"><xsl:text>&#120821;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120314;</xsl:text></xsl:if>
	<xsl:if test=".='8'"><xsl:text>&#120820;</xsl:text></xsl:if>
	<xsl:if test=".='χ'"><xsl:text>&#120710;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120313;</xsl:text></xsl:if>
	<xsl:if test=".='4'"><xsl:text>&#120816;</xsl:text></xsl:if>
	<xsl:if test=".='κ'"><xsl:text>&#120697;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120317;</xsl:text></xsl:if>
	<xsl:if test=".='ψ'"><xsl:text>&#120711;</xsl:text></xsl:if>
	<xsl:if test=".='η'"><xsl:text>&#120694;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120293;</xsl:text></xsl:if>
	<xsl:if test=".='Ζ'"><xsl:text>&#120667;</xsl:text></xsl:if>
	<xsl:if test=".='5'"><xsl:text>&#120817;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120316;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='sans-serif'">
	<xsl:if test=".='S'"><xsl:text>&#120242;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120250;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120243;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120237;</xsl:text></xsl:if>
	<xsl:if test=".='7'"><xsl:text>&#120809;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120234;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120253;</xsl:text></xsl:if>
	<xsl:if test=".='2'"><xsl:text>&#120804;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120248;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120228;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120259;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120274;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120249;</xsl:text></xsl:if>
	<xsl:if test=".='1'"><xsl:text>&#120803;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120270;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120260;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120256;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120269;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120254;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120233;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120246;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120271;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120268;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120225;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120231;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120252;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120266;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120251;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120227;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120232;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120230;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120275;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120244;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120272;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120229;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120267;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120273;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120245;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120240;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120257;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120236;</xsl:text></xsl:if>
	<xsl:if test=".='0'"><xsl:text>&#120802;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120226;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120235;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120255;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120258;</xsl:text></xsl:if>
	<xsl:if test=".='6'"><xsl:text>&#120808;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120224;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120263;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120238;</xsl:text></xsl:if>
	<xsl:if test=".='3'"><xsl:text>&#120805;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120247;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120239;</xsl:text></xsl:if>
	<xsl:if test=".='9'"><xsl:text>&#120811;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120262;</xsl:text></xsl:if>
	<xsl:if test=".='8'"><xsl:text>&#120810;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120261;</xsl:text></xsl:if>
	<xsl:if test=".='4'"><xsl:text>&#120806;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120265;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120241;</xsl:text></xsl:if>
	<xsl:if test=".='5'"><xsl:text>&#120807;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120264;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='bold-script'">
	<xsl:if test=".='S'"><xsl:text>&#120034;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120042;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120035;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120029;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120026;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120045;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120040;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120020;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120051;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120066;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120041;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120062;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120052;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120048;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120061;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120046;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120025;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120038;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120063;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120060;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120017;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120023;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120044;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120058;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120043;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120019;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120024;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120022;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120067;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120036;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120064;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120021;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120059;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120065;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120037;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120032;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120049;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120028;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120018;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120027;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120047;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120050;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120016;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120055;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120030;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120039;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120031;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120054;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120053;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120057;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120033;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120056;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='italic'">
	<xsl:if test=".='ο'"><xsl:text>&#120586;</xsl:text></xsl:if>
	<xsl:if test=".='S'"><xsl:text>&#119878;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#119870;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#119889;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#119884;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#119864;</xsl:text></xsl:if>
	<xsl:if test=".='Τ'"><xsl:text>&#120565;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#119910;</xsl:text></xsl:if>
	<xsl:if test=".='Π'"><xsl:text>&#120561;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#119892;</xsl:text></xsl:if>
	<xsl:if test=".='δ'"><xsl:text>&#120575;</xsl:text></xsl:if>
	<xsl:if test=".='Κ'"><xsl:text>&#120555;</xsl:text></xsl:if>
	<xsl:if test=".='ω'"><xsl:text>&#120596;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#119890;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#119869;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#119902;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#119887;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#119863;</xsl:text></xsl:if>
	<xsl:if test=".='Ω'"><xsl:text>&#120570;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#119911;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#119908;</xsl:text></xsl:if>
	<xsl:if test=".='΢'"><xsl:text>&#120563;</xsl:text></xsl:if>
	<xsl:if test=".='Θ'"><xsl:text>&#120553;</xsl:text></xsl:if>
	<xsl:if test=".='Ι'"><xsl:text>&#120554;</xsl:text></xsl:if>
	<xsl:if test=".='Ν'"><xsl:text>&#120558;</xsl:text></xsl:if>
	<xsl:if test=".='Ε'"><xsl:text>&#120550;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#119876;</xsl:text></xsl:if>
	<xsl:if test=".='ς'"><xsl:text>&#120589;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#119872;</xsl:text></xsl:if>
	<xsl:if test=".='Ψ'"><xsl:text>&#120569;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#119862;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#119871;</xsl:text></xsl:if>
	<xsl:if test=".='σ'"><xsl:text>&#120590;</xsl:text></xsl:if>
	<xsl:if test=".='ζ'"><xsl:text>&#120577;</xsl:text></xsl:if>
	<xsl:if test=".='θ'"><xsl:text>&#120579;</xsl:text></xsl:if>
	<xsl:if test=".='Ο'"><xsl:text>&#120560;</xsl:text></xsl:if>
	<xsl:if test=".='Γ'"><xsl:text>&#120548;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#119883;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#119875;</xsl:text></xsl:if>
	<xsl:if test=".='ι'"><xsl:text>&#120580;</xsl:text></xsl:if>
	<xsl:if test=".='Ρ'"><xsl:text>&#120562;</xsl:text></xsl:if>
	<xsl:if test=".='ε'"><xsl:text>&#120576;</xsl:text></xsl:if>
	<xsl:if test=".='Δ'"><xsl:text>&#120549;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#119879;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#119886;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#119873;</xsl:text></xsl:if>
	<xsl:if test=".='ρ'"><xsl:text>&#120588;</xsl:text></xsl:if>
	<xsl:if test=".='Φ'"><xsl:text>&#120567;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#119895;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#119885;</xsl:text></xsl:if>
	<xsl:if test=".='Σ'"><xsl:text>&#120564;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#119906;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#119896;</xsl:text></xsl:if>
	<xsl:if test=".='Α'"><xsl:text>&#120546;</xsl:text></xsl:if>
	<xsl:if test=".='Η'"><xsl:text>&#120552;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#119905;</xsl:text></xsl:if>
	<xsl:if test=".='λ'"><xsl:text>&#120582;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#119882;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#119907;</xsl:text></xsl:if>
	<xsl:if test=".='τ'"><xsl:text>&#120591;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#119861;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#119904;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#119867;</xsl:text></xsl:if>
	<xsl:if test=".='ν'"><xsl:text>&#120584;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#119888;</xsl:text></xsl:if>
	<xsl:if test=".='ξ'"><xsl:text>&#120585;</xsl:text></xsl:if>
	<xsl:if test=".='β'"><xsl:text>&#120573;</xsl:text></xsl:if>
	<xsl:if test=".='Μ'"><xsl:text>&#120557;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#119868;</xsl:text></xsl:if>
	<xsl:if test=".='Λ'"><xsl:text>&#120556;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#119866;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#119880;</xsl:text></xsl:if>
	<xsl:if test=".='γ'"><xsl:text>&#120574;</xsl:text></xsl:if>
	<xsl:if test=".='α'"><xsl:text>&#120572;</xsl:text></xsl:if>
	<xsl:if test=".='Υ'"><xsl:text>&#120566;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#119865;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#119903;</xsl:text></xsl:if>
	<xsl:if test=".='Χ'"><xsl:text>&#120568;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#119909;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#119881;</xsl:text></xsl:if>
	<xsl:if test=".='Ξ'"><xsl:text>&#120559;</xsl:text></xsl:if>
	<xsl:if test=".='μ'"><xsl:text>&#120583;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#8462;</xsl:text></xsl:if>
	<xsl:if test=".='φ'"><xsl:text>&#120593;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#119891;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#119894;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#119860;</xsl:text></xsl:if>
	<xsl:if test=".='Β'"><xsl:text>&#120547;</xsl:text></xsl:if>
	<xsl:if test=".='π'"><xsl:text>&#120587;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#119874;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#119899;</xsl:text></xsl:if>
	<xsl:if test=".='υ'"><xsl:text>&#120592;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#119898;</xsl:text></xsl:if>
	<xsl:if test=".='χ'"><xsl:text>&#120594;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#119897;</xsl:text></xsl:if>
	<xsl:if test=".='κ'"><xsl:text>&#120581;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#119901;</xsl:text></xsl:if>
	<xsl:if test=".='ψ'"><xsl:text>&#120595;</xsl:text></xsl:if>
	<xsl:if test=".='η'"><xsl:text>&#120578;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#119877;</xsl:text></xsl:if>
	<xsl:if test=".='Ζ'"><xsl:text>&#120551;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#119900;</xsl:text></xsl:if>
</xsl:when>
<xsl:when test="@mathvariant='bold-fraktur'">
	<xsl:if test=".='S'"><xsl:text>&#120190;</xsl:text></xsl:if>
	<xsl:if test=".='a'"><xsl:text>&#120198;</xsl:text></xsl:if>
	<xsl:if test=".='T'"><xsl:text>&#120191;</xsl:text></xsl:if>
	<xsl:if test=".='N'"><xsl:text>&#120185;</xsl:text></xsl:if>
	<xsl:if test=".='K'"><xsl:text>&#120182;</xsl:text></xsl:if>
	<xsl:if test=".='d'"><xsl:text>&#120201;</xsl:text></xsl:if>
	<xsl:if test=".='Y'"><xsl:text>&#120196;</xsl:text></xsl:if>
	<xsl:if test=".='E'"><xsl:text>&#120176;</xsl:text></xsl:if>
	<xsl:if test=".='j'"><xsl:text>&#120207;</xsl:text></xsl:if>
	<xsl:if test=".='y'"><xsl:text>&#120222;</xsl:text></xsl:if>
	<xsl:if test=".='Z'"><xsl:text>&#120197;</xsl:text></xsl:if>
	<xsl:if test=".='u'"><xsl:text>&#120218;</xsl:text></xsl:if>
	<xsl:if test=".='k'"><xsl:text>&#120208;</xsl:text></xsl:if>
	<xsl:if test=".='g'"><xsl:text>&#120204;</xsl:text></xsl:if>
	<xsl:if test=".='t'"><xsl:text>&#120217;</xsl:text></xsl:if>
	<xsl:if test=".='e'"><xsl:text>&#120202;</xsl:text></xsl:if>
	<xsl:if test=".='J'"><xsl:text>&#120181;</xsl:text></xsl:if>
	<xsl:if test=".='W'"><xsl:text>&#120194;</xsl:text></xsl:if>
	<xsl:if test=".='v'"><xsl:text>&#120219;</xsl:text></xsl:if>
	<xsl:if test=".='s'"><xsl:text>&#120216;</xsl:text></xsl:if>
	<xsl:if test=".='B'"><xsl:text>&#120173;</xsl:text></xsl:if>
	<xsl:if test=".='H'"><xsl:text>&#120179;</xsl:text></xsl:if>
	<xsl:if test=".='c'"><xsl:text>&#120200;</xsl:text></xsl:if>
	<xsl:if test=".='q'"><xsl:text>&#120214;</xsl:text></xsl:if>
	<xsl:if test=".='b'"><xsl:text>&#120199;</xsl:text></xsl:if>
	<xsl:if test=".='D'"><xsl:text>&#120175;</xsl:text></xsl:if>
	<xsl:if test=".='I'"><xsl:text>&#120180;</xsl:text></xsl:if>
	<xsl:if test=".='G'"><xsl:text>&#120178;</xsl:text></xsl:if>
	<xsl:if test=".='z'"><xsl:text>&#120223;</xsl:text></xsl:if>
	<xsl:if test=".='U'"><xsl:text>&#120192;</xsl:text></xsl:if>
	<xsl:if test=".='w'"><xsl:text>&#120220;</xsl:text></xsl:if>
	<xsl:if test=".='F'"><xsl:text>&#120177;</xsl:text></xsl:if>
	<xsl:if test=".='r'"><xsl:text>&#120215;</xsl:text></xsl:if>
	<xsl:if test=".='x'"><xsl:text>&#120221;</xsl:text></xsl:if>
	<xsl:if test=".='V'"><xsl:text>&#120193;</xsl:text></xsl:if>
	<xsl:if test=".='Q'"><xsl:text>&#120188;</xsl:text></xsl:if>
	<xsl:if test=".='h'"><xsl:text>&#120205;</xsl:text></xsl:if>
	<xsl:if test=".='M'"><xsl:text>&#120184;</xsl:text></xsl:if>
	<xsl:if test=".='C'"><xsl:text>&#120174;</xsl:text></xsl:if>
	<xsl:if test=".='L'"><xsl:text>&#120183;</xsl:text></xsl:if>
	<xsl:if test=".='f'"><xsl:text>&#120203;</xsl:text></xsl:if>
	<xsl:if test=".='i'"><xsl:text>&#120206;</xsl:text></xsl:if>
	<xsl:if test=".='A'"><xsl:text>&#120172;</xsl:text></xsl:if>
	<xsl:if test=".='n'"><xsl:text>&#120211;</xsl:text></xsl:if>
	<xsl:if test=".='O'"><xsl:text>&#120186;</xsl:text></xsl:if>
	<xsl:if test=".='X'"><xsl:text>&#120195;</xsl:text></xsl:if>
	<xsl:if test=".='P'"><xsl:text>&#120187;</xsl:text></xsl:if>
	<xsl:if test=".='m'"><xsl:text>&#120210;</xsl:text></xsl:if>
	<xsl:if test=".='l'"><xsl:text>&#120209;</xsl:text></xsl:if>
	<xsl:if test=".='p'"><xsl:text>&#120213;</xsl:text></xsl:if>
	<xsl:if test=".='R'"><xsl:text>&#120189;</xsl:text></xsl:if>
	<xsl:if test=".='o'"><xsl:text>&#120212;</xsl:text></xsl:if>
</xsl:when>


					<xsl:otherwise>
						<xsl:value-of select="." />
					</xsl:otherwise>
				</xsl:choose>
				</mo>
				</xsl:template>
</xsl:stylesheet>
<!-- 
generating program for matvariant to utf-8:
 use utf8; 
#  use Unicode::UCD 'charinfo';
#  use Unicode::String; #apt-get install libunicode-string-perl
#  use Encode;
#use Data::Dumper;
#use HTML::Entities;
 
sub UTF {
  my($code)=@_;
  pack('U',$code); }

sub makePlane1Map {
  my($latin,$GREEK,$greek,$digits)=@_; 
  ( map( (UTF(ord('A')+ $_)=>UTF($latin + $_)), 0..25),
    map( (UTF(ord('a')+ $_)=>UTF($latin + 26 + $_)), 0..25),
    ($GREEK ? map( (UTF(0x0391+ $_)=>UTF($GREEK + $_)), 0..24) : ()),
    ($greek ? map( (UTF(0x03B1+ $_)=>UTF($greek + $_)), 0..24) : ()),
    ($digits ? map( (UTF(ord('0')+ $_)=>UTF($digits + $_)), 0..9) : ())); }

our %plane1map =
     ('bold'                  =>{makePlane1Map(0x1D400,0x1D6A8,0x1D6C2,0x1D7CE)},
      'italic'                =>{makePlane1Map(0x1D434,0x1D6E2,0x1D6FC, undef),
				h=>"\x{210E}"},
      'bold-italic'           =>{makePlane1Map(0x1D468,0x1D71C,0x1D736, undef)},
      'sans-serif'            =>{makePlane1Map(0x1D5A0, undef,  undef, 0x1D7E2)},
      'bold-sans-serif'       =>{makePlane1Map(0x1D5D4,0x1D756,0x1D770,0x1D7EC)},
      'sans-serif-italic'     =>{makePlane1Map(0x1D608, undef,  undef,  undef)},
      'sans-serif-bold-italic'=>{makePlane1Map(0x1D63C,0x1D790,0x1D7AA, undef)},
      'monospace'             =>{makePlane1Map(0x1D670, undef,  undef, 0x1D7F6)},
      'script'                =>{makePlane1Map(0x1D49C, undef,  undef,  undef),
				 B=>"\x{212C}",E=>"\x{2130}",F=>"\x{2131}",H=>"\x{210B}",I=>"\x{2110}",
				 L=>"\x{2112}",M=>"\x{2133}",R=>"\x{211B}",
				 e=>"\x{212F}",g=>"\x{210A}",o=>"\x{2134}"},
      'bold-script'           =>{makePlane1Map(0x1D4D0, undef,  undef,  undef)},
      'fraktur'               =>{makePlane1Map(0x1D504, undef,  undef,  undef),
				 C=>"\x{212D}",H=>"\x{210C}",I=>"\x{2111}",R=>"\x{211C}", Z=>"\x{2128}"},
      'bold-fraktur'          =>{makePlane1Map(0x1D56C, undef,  undef,  undef)},
      'double-struck'         =>{makePlane1Map(0x1D538, undef,  undef, 0x1D7D8),
				 C=>"\x{2102}",H=>"\x{210D}",N=>"\x{2115}",P=>"\x{2119}",Q=>"\x{211A}",
				 R=>"\x{211D}",Z=>"\x{2124}"}
    );

our %plane1hack = (script=>$plane1map{script},  'bold-script'=>$plane1map{script},
		   fraktur=>$plane1map{fraktur},'bold-fraktur'=>$plane1map{fraktur},
		   'double-struck'=>$plane1map{'double-struck'});

		   binmode STDOUT, ":utf8";
while (($key, $value) = each(%plane1map)){
	print '<xsl:when test="@mathvariant=\''.$key.'\'">'."\n";
	while(($key2, $value2) = each(%$value)){
		my $UC = unpack("U",$value2);
		#my $hex = sprintf "%x",$UC;
		#print $UC.$value2.chr($UC).pack("U",$value2)."\n";
		#my $html = '&#'.hex.';';
		#decode_entities($html);
		#print Dumper(charinfo($UC));
		
		#print Dumper(decode('latin1', chr(unpack('U',$value2))));
		print "\t".'<xsl:if test=".=\''.$key2.'\'"><xsl:text>&#'.$UC.';</xsl:text></xsl:if>'."\n";
	}
print '</xsl:when>'."\n";
}


 -->