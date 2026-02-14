## ANNUAL PHYSICIAN FEE SCHEDULE PAYMENT AMOUNT FILE

## (DOWNLOADABLE VERSION)

## CALENDAR YEAR 202 6

Contents:

```
File
Organization:
```
```
Initial
Source
```
```
Data Set Name:
```
```
This file contains locality -specific physician fee schedule payment
amounts for services cover ed by the Medicare Physician Fee Schedule
(MPFS).
```
```
This file contains one record for each unique combination of
carrier, locality, procedure code and modifier and is sorted in the above
listed code sequence.
```
```
November 202 5 Federal Register publications of Final Rule of Medicare
Program's Fee Schedule for Physicians' Services for Calendar Year
2026.
```
```
There is one file available: PFALL2 6 A.ZIP contains pricing data for the
entire country.
```
```
This is a self-extracting compressed fil e which when decompressed will
contain two files:
```
```
(1)PF2 6 PA (in PDF (.pdf) formats) contains the file's record
layout and file documentation; and
(2)PFALL2 6 A.TXT which is an ASCII text file containing the
applicable physician fee schedule pricing information.
```
Update The file is available for the forthcoming calendar year upon
Schedule: publication of the final rule in the Federal Register, which is usually in
early November. Additionally, this file will be updated on a periodic
schedule to incorporate mid-year changes. Updated files will be available
on April 1, July 1, and October 1.

```
These changes will be provided in separate revision files; they are not
overlay files. The following naming convention will be used to identify the
revision files:
```
```
First revision release
Second revision release
Third revision release
```
## PFREV2 6 A.ZIP

## PFREV2 6 B.ZIP

## PFREV2 6 C.ZIP

```
PFREV2 6 D.ZIP Fourth revision release
```
NO T E:

o CPT codes and descriptions only are copyright 202 6 by the American Medical
Association. All Rights Reserved. Applicable FARS/DFARS apply.
o Dental codes (D codes) are copyright 2025/ 26 American Dental Association.
All Rights Reserved.



## ANNUAL PHYSICIAN FEE SCHEDULE PAYM ENT AM OUNT FILE

## DATA ELEM ENT LOCATION

## COBOL

## PIC DESCRIPTION^

## DATA RECORD

Filler 1 - 1 x( 1 ) Value Quote
Year 2 - 5 x( 4 ) Effective Calendar Year for Payment Amounts
Filler 6 - 8 x( 3 ) Value Quote, Comma, Quote
Carrier Number 9 - 13 x( 5 ) HCF A-Assigned Identification Number
Filler 14 - 16 x( 3 ) Value Quote, Comma, Quote
Locality 17 - 18 x( 2 ) Identification of Pricing Locality
Filler 19 - 21 x( 3 ) Value Quote, Comma, Quote
HCPCS Code 22 - 26 x( 5 ) CPT or Level 2 HCPCS code number for the service. NOTE: See copyright statement
on cover sheet.
Filler 27 - 29 x( 3 ) Value Quote, Comma, Quote
Modifier 30 - 31 x( 2 ) For diagnostic tests, a blank in this field denotes the global service and the following
modifiers identify the components:
--26 = Professional component
--TC = Technical component
For services other than those with a professional and/or technical component, a blank
will appear in this field with one exception: the presence of CPT modifier -53 indicates
that separate RVUs and a fee schedule amount have been established for procedures
which the physician terminated before completion. In the 2004 MPFSDB, this modifier
is used only with colonoscopy code 45378, G0105, and G0121. Any other codes billed
with modifier –53 are subject to carrier medical review and priced by individual
consideration.
Filler 32 - 34 x( 3 ) Value Quote, Comma, Quote


Non Facility Fee
Schedule Amount

```
35 - 44 9(7).99 Pricing amount for the non-facility setting. If applicable, the payment amount has
been capped at the level of the OPPS Payment Amount mandated by Section 5102(b)
of the Deficit Reduction Act of 2005 (See column 107-116 for the OPPS Amounts that
are to be used to reduce these payment amounts.).
```
```
For a description of the computation of the full fee schedule amount, refer to the
Federal Register Final Rule for the Medicare Physician Fee Schedule.
```
The Medicare limiting charge is set by law at 115 percent of the payment amount for the
service furnished by the nonparticipating physician. However, the law sets the payment
amount for nonparticipating physicians at 95 percent of the payment amount for
participating physicians (i.e. the full fee schedule amount). Calculating 95 percent of 11
percent of an amount is equivalent to multiplying the amount by a factor of 1.0925 (or
109.25 percent). Therefore, to calculate the Medicare limiting charge for a physician
service for a locality, multiply the full fee schedule amount by a factor of 1.0925. The
result is the Medicare limiting charge for that service for that locality to which the full fee
schedule amount applies.
Filler 45 - 47 x( 3 ) Value Quote, Comma, Quote
Facility Fee
Schedule
Amount

```
48 - 57 9(7).99 Pricing amount for the facility setting. If applicable, the payment amount has been
capped at the level of the OPPS Payment Amount mandated by Section 5102(b) of the
Deficit Reduction Act of 2005 (See column 120-129 for the OPPS Amounts that are to
be used to reduce these payment amounts.).
```
Filler 58 - 60 x( 3 ) Value Quote, Comma, Quote
Filler 61 - 61 x( 1 )
Filler 62 - 64 x( 3 ) Value Quote, Comma, Quote
PCTC Indicator 65 - 65 x( 1 )
Filler 66 - 68 x( 3 ) Value Quote, Comma, Quote
Status Code 69 - 69 x( 1 )
Filler 70 - 72 x( 3 ) Value Quote, Comma, Quote
Multiple Surgery
Indicator

```
73 - 73 x( 1 )
```
Filler 74 - 76 x( 3 ) Value Quote, Comma, Quote


50% Therapy
Reduction Amount

77 - 86 9(7).99 Pricing amount that reflects 50 percent payment for the PE for services furnished in
office and other noninstitutional settings (services paid under section 1848 of the Act).
Flat Visit Fee Effective January 1, 2021, this field may also contain the Flat Visit Fee for the Primary
Care First Model (FFS 11896).
Filler 87 - 89 x( 3 ) Value Quote, Comma, Quote
50% Therapy
Reduction Amount

90 - 99 9(7).99 Pricing amount that reflects 50 percent payment for the PE for services furnished in an
institutional setting (services paid under section 1834 of the Act).
Filler 100 - 102 x( 3 ) Value Quote, Comma, Quote
OPPS Indicator 103 - 103 x( 1 )
Filler 104 - 106 x( 3 ) Value Quote, Comma, Quote
OPPS Non Facility
Fee Amount

107 - 116 9(7).99 Pricing amount for the non-facility setting that has been capped at the level of the
OPPS Payment Amount mandated by Section 5102(b) of the Deficit Reduction Act of
2005.
Filler 117 - 119 x( 3 ) Value Quote, Comma, Quote
OPPS Facility Fee
Amount

```
120 - 129 9(7).99 Pricing amount for the facility setting that has been capped at the level of the OPPS
Payment Amount mandated by Section 5102(b) of the Deficit Reduction Act of 2005.
```
Filler 130 - 130 x( 1 ) Value Quote
TRAILER
RECORD
Filler 1 - 1 x( 1 ) Value Quote
Trailer Indicator 2 - 4 x( 3 ) Value TRL.
Copyright
Statement

```
5 - 98 x(94)
```

## ATTACHMENT A

CARRIER

STATE
ACRONYM NAME
1011200 AL ALABAMA
1021201 GA ATLANTA, GA
1021299 GA REST OF GEORGIA
0710213 AR ARKANSAS
0421205 NM NEW MEXICO
0431200 OK OKLAHOMA
0530201 MO METROPOLITAN ST. LOUIS, MO
0720201 LA NEW ORLEANS, LA
0720299 LA REST OF LOUISIANA
1210201 DE DELAWARE
1220201 DC DC + MD/VA SUBURBS
0910203 FL FORT LAUDERDALE, FL
0910204 FL MIAMI, FL
0910299 FL REST OF FLORIDA
0810200 IN INDIANA
0510200 IA IOW A
0520200 KS KANSAS
0540200 NE NEBRASKA
1510200 KY KENTUCKY
0530202 MO METROPOLITAN KANSAS CITY, MO
0530299 MO REST OF MISSOURI*
0320201 MT MONTANA
1328299 NY1 REST OF NEW YORK
1320201 NY2 MANHATTAN, NY
1320202 NY2 NYC SUBURBS/LONG I., NY
1320203 NY2 POUGHKPSIE/N NYC SUBURBS, NY
1240201 NJ NORTHERN NJ
1240299 NJ REST OF NEW JERSEY
0330201 ND NORTH DAKOTA
0340202 SD SOUTH DAKOTA
0360221 WY W YOMING
0240202 WA SEATTLE (KING CNTY), WA
0240299 WA REST OF WASHINGTON
0210201 AK ALASKA
0310200 AZ ARIZONA
0131200 NV NEVADA
0411201 CO COLORADO
0121201 HI HAWAII/GUAM
0230201 OR PORTLAND, OR
0230299 OR REST OF OREGON
1250201 PA METROPOLITAN PHILADELPHIA, PA
1250299 PA REST OF PENNSYLVANIA
1441201 RI RHODE ISLAND
1120201 SC SOUTH CAROLINA
0441209 TX BRAZORIA, TX


0441211 TX DALLAS, TX
0441215 TX GALVESTON, TX
0441218 TX HOUSTON, TX
0441220 TX BEAUMONT, TX
0441228 TX FORT W ORTH, TX
0441231 TX A US TIN, TX
0441299 TX REST OF TEXAS
1230201 MD BALTIMORE/SURR. CNTYS, MD
1230299 MD REST OF MARYLAND
0350209 UT UTA H
0630200 WI W ISCONSIN
0610212 IL EAST ST. LOUIS, IL
0610215 IL SUBURBAN CHICAGO, IL
0610216 IL CHICAGO, IL
0610299 IL REST OF ILLINOIS
0820201 MI DE TROIT, M I
0820299 MI REST OF MICHIGAN
0920220 PRV PUERTO RICO
0920250 PRV VIRGIN ISLANDS
0118217 CA2 VENTURA, CA
0118218 CA2 LOS ANGELES, CA
0118226 CA2 ANAHEIM/SANTA ANA, CA
0118271 CA2 EL CENTRO
0118272 CA2 SAN DIEGO – CARSLBAD

0118273 CA

SAN LUIS OBISPO-PASO ROBLES-ARROYO
GRANDE
0118274 CA2 SANTA MARIA-SANTA BARBARA
0118275 CA2 REST OF STATE
0220200 ID IDAHO
1031235 TN TENNESSEE
1150200 NC NORTH CAROLINA
1310200 CT CONNECTICUT
0620200 MN MINNESOTA
0730200 MS MISSISSIPPI
1130200 VA VIRGINIA
1329204 NY3 QUEENS, NY
1520200 OH OHIO
1140216 WV W EST VIRGINIA
0111205 CA1 SAN FRANCISCO, CA
0111206 CA1 SAN MATEO, CA
0111207 CA1 OAKLAND/BERKELEY, CA
0111209 CA1 SANTA CLARA, CA

(^0111251) CA1 NAPA
0111252
CA
SAN FRANCISCO-OAKLAND-HAYWARD (MARIN
CNTY)
(^0111253) CA1 VALLEJO-FAIRFIELD
(^0111254) CA1 BAKERSFIELD
(^0111255) CA1 CHICO
0111256 CA1 FRESNO
(^0111257) CA1 HANFORD-CORCORAN


0111258 CA1 MADERA

(^0111259) CA1 MERCED
0111260 CA1 MODESTO
0111261 CA1 REDDING
(^0111262) CA1 RIVERSIDE-SAN BERNARDINO-ONTARIO
(^0111263) CA1 SACRAMENTO--ROSEVILLE--ARDEN-ARCADE
0111264 CA1 SALINAS
0111265
CA
SAN JOSE-SUNNYVALE-SANTA CLARA (SAN
BENITO CNTY)
0111266 CA1 SANTA CRUZ-WATSONVILLE
0111267 CA1 SANTA ROSA
0111268 CA1 S TOCK TON-LODI
0111269 CA1 VISALIA-PORTERVILLE
(^0111270) CA1 YUBA CITY
1411203 ME SOUTHERN MAINE
1411299 ME REST OF MAINE
1421201 MA METROPOLITAN BOSTON
1421299 MA REST OF MASSACHUSETTS
1431240 NH NEW HAMPSHIRE
1451250 VT VERMONT


## ATTACHMENT B

STATUS CODE A = Active Code. These codes are paid separately under the physician fee
schedule, if covered. There will be RVUs for codes with this status.
The presence of an "A" indicator does not mean that Medicare has
made a national coverage determination regarding the service; carriers
remain responsible for coverage decisions in the absence of a national
Medicare policy.

```
B = Bundled Code. Payment for covered services are always bundled into
payment for other services not specified. If RVUs are shown, they are
not used for Medicare payment. If these services are covered, payment
for them is subsumed by the payment for the services to which they are
incident. (An example is a telephone call from a hospital nurse
regarding care of a patient).
```
```
C = Carriers price the code. Carriers will establish RVUs and payment
amounts for these services, generally on an individual case basis
following review of documentation such as an operative report.
```
```
D = Deleted Codes. These codes are deleted effective with the beginning
of the applicable year. These codes will not appear on the 2008 file as
the grace period for deleted codes is no longer applicable.
```
```
E = Excluded from Physician Fee Schedule by regulation. These codes are
for items and/or services that CMS chose to exclude from the fee
schedule payment by regulation. No RVUs are shown, and no payment
may be made under the fee schedule for these codes. Payment for
them, when covered, generally continues under reasonable charge
procedures.
```
```
F = Deleted/Discontinued Codes. (Code not subject to a 90 day grace
period). These codes will not appear on the 2008 file as the grace
period for deleted codes is no longer applicable.
```
```
G = Not valid for Medicare purposes. Medicare uses another code for
reporting of, and payment for, these services. (Code subject to a 90
day grace period.) These codes will not appear on the 2008 file as the
grace period for deleted codes is no longer applicable.
```
```
H = Deleted Modifier. This code had an associated TC and/or 26 modifier in
the previous year. For the current year, the TC or 26 component shown
for the code has been deleted, and the deleted component is shown
with a status code of "H". These codes will not appear on the 2008 file
as the grace period for deleted codes is no longer applicable.
```
```
I = Not valid for Medicare purposes. Medicare uses another code for
reporting of, and payment for, these services. (Code NO T subject to a
90 day grace period.)
```

```
J = Anesthesia Services. There are no RVUs and no payment amounts for
these codes. The intent of this value is to facilitate the identification of
anesthesia services.
```
```
M = Measurement codes. Used for reporting purposes only.
```
```
N = No n-covered Services. These services are not covered by Medicare.
```
```
P = Bundled/Excluded Codes. There are no RVUs and no payment
amounts for these services. No separate payment should be made for
them under the fee schedule.
--If the item or service is covered as incident to a physician service
and is provided on the same day as a physician service, payment
for it is bundled into the payment for the physician service to which it
is incident. (An example is an elastic bandage furnished by a
physician incident to physician service.)
--If the item or service is covered as other than incident to a
physician service, it is excluded from the fee schedule (i.e.,
colostomy supplies) and should be paid under the other payment
provision of the Act.
```
```
R = Restricted Coverage. Special coverage instructions apply. If no RVUs
are shown, the service is carrier priced. (NOTE: The majority of codes
to which this indicator will be assigned are the alpha-numeric dental
codes, which begin with "D". We are assigning the indicator to a limited
number of CPT codes which represent services that are covered only in
unusual circumstances.)
```
```
T = Injections. There are RVUS and payment amounts for these services,
but they are only paid if there are no other services payable under the
physician fee schedule billed on the same date by the same provider. If
any other services payable under the physician fee schedule are billed
on the same date by the same provider, these services are bundled into
the physician services for which payment is made. (NOTE: This is a
change from the previous definition, which states that injection services
are bundled into any other services billed on the same date.)
```
X = Statutory Exclusion. These codes represent an item or service that is
not in the statutory definition of "physician services" for fee schedule
payment purposes. No RVUS or payment amounts are shown for these
codes, and no payment may be made under the physician fee
schedule. (Examples are ambulance services and clinical diagnostic
laboratory services.)


PC/TC INDICAT OR 0 = Physician Service Codes--Identifies codes that describe
physician services. Examples include visits, consultations, and
surgical procedures. The concept of PC/TC does not apply
since physician services cannot be split into professional and
technical components. Modifiers 26 and TC cannot be used
with these codes. The RVUS include values for physician work,
practice expense and malpractice expense. There are some
codes with no work RVUs.

```
1 = Diagnostic Tests for Radiology Services--Identifies codes that
describe diagnostic tests. Examples are pulmonary function
tests or therapeutic radiology procedures, e.g., radiation therapy.
These codes have both a professional and technical component.
Modifiers 26 and TC can be used with these codes. The total
RVUs for codes reported with a 26 modifier include values for
physician work, practice expense, and malpractice expense.
The total RVUs for codes reported with a TC modifier include
values for practice expense and malpractice expense only. The
total RVUs for codes reported without a modifier include values
for physician work, practice expense, and malpractice expense.
```
```
2 = Professional Component Only Codes--T his indicator identifies
stand-alone codes that describe the physician work portion of
selected diagnostic tests for which there is an associated code
that describes the technical component of the diagnostic test
only and another associated code that describes the global test.
An example of a professional component only code is CPT code
93010--Electrocardiogram; Interpretation and Report.
Modifiers 26 and TC cannot be used with these codes. The total
RVUs for professional component only codes include values for
physician work, practice expense, and malpractice expense.
```
```
3 = Technical Component Only Codes--This indicator identifies
stand- alone codes that describe the technical component (i.e.,
staff and equipment costs) of selected diagnostic tests for which
there is an associated code that describes the professional
component of the diagnostic test only. An example of a
technical component only code is CPT code 93005--
Electrocardiogram; Tracing Only, without interpretation and
report. It also identifies codes that are covered only as
diagnostic tests and therefore do not have a related professional
code. Modifiers 26 and TC cannot be used with these codes.
The total RVUs for technical component only codes include
values for practice expense and malpractice expense only.
```

4 = Global Test Only Codes--This indicator identifies stand-alone
codes that describe selected diagnostic tests for which there are
associated codes that describe (a) the professional component
of the test only, and (b) the technical component of the test only.
Modifiers 26 and TC cannot be used with these codes. The total
RVUs for global procedure only codes include values for
physician work, practice expense, and malpractice expense.
The total RVUs for global procedure only codes equals the sum
of the total RVUs for the professional and technical components
only codes combined.

5 = Incident To Codes--This indicator identifies codes that describe
services covered incident to a physician's service when they are
provided by auxiliary personnel employed by the physician and
working under his or her direct personal supervision. Payment
may not be made by carriers for these services when they are
provided to hospital inpatients or patients in a hospital outpatient
department. Modifiers 26 and TC cannot be used with these
codes.

6 = Laboratory Physician Interpretation Codes--This indicator
identifies clinical laboratory codes for which separate payment
for interpretations by laboratory physicians may be made.
Actual performance of the tests is paid for under the lab fee
schedule. Modifier TC cannot be used with these codes. The
total RVUs for laboratory physician interpretation codes include
values for physician work, practice expense, and malpractice
expense.

7 = Physical therapy service, for which payment may not be made--
Payment may not be made if the service is provided to either a
patient in a hospital outpatient department or to an inpatient of
the hospital by an independently practicing physical or
occupational therapist.

8 = Physician interpretation codes: This indicator identifies the
professional component of clinical laboratory codes for which
separate payment may be made only if the physician interprets
an abnormal smear for hospital inpatient. This applies to CPT
codes 88141, 85060 and HCPCS code P3001-26. No TC billing
is recognized because payment for the underlying clinical
laboratory test is made to the hospital, generally through the
PPS rate.

```
No payment is recognized for CPT codes 88141, 85060 or
HCPCS code P3001-26 furnished to hospital outpatients or non-
hospital patients. The physician interpretation is paid through
the clinical laboratory fee schedule payment for the clinical
laboratory test.
```
```
9 = Not Applicable--Concept of a professional/technical component
does not apply.
```

Multiple Surgery Indicator indicates which payment adjustment rule for multiple procedures
applies to the service.

```
0 = No payment adjustment rules for multiple procedures apply. If
procedure is reported on the same day as another procedure,
base payment on the lower of: (a) the actual charge or (b) the fee
schedule amount for the procedure.
```
```
1 = Standard payment adjustment rules in effect before January 1,
1996, for multiple procedures apply. In the 1996 MPFSDB, this
indicator only applies to codes with procedure status of “D.” If a
procedure is reported on the same day as another procedure with
an indicator of 1,2, or 3, rank the procedures by fee schedule
amount and apply the appropriate reduction to this code (
percent, 50 percent, 25 percent, 25 percent, 25 percent, and by
report). Base payment on the lower of: (a) the actual charge or (b)
the fee schedule amount reduced by the appropriate percentage.
```
```
2 = Standard payment adjustment rules for multiple procedures
apply. If procedure is reported on the same day as another
procedure with an indicator of 1, 2, or 3, rank the procedures by
fee schedule amount and apply the appropriate reduction to this
code (100 percent, 50 percent, 50 percent, 50 percent, 50
percent, and by report). Base payment on the lower of: (a) the
actual charge or (b) the fee schedule amount reduced by the
appropriate percentage.
3 = Special rules for multiple endoscopic procedures apply if
procedure is billed with another endoscopy in the same family
(i.e., another endoscopy that has the same base procedure). The
base procedure for each code with this indicator is identified in
field 31G.
Apply the multiple endoscopy rules to a family before ranking the
family with other procedures performed on the same day (for
example, if multiple endoscopies in the same family are reported
on the same day as endoscopies in another family or on the same
day as a non-endoscopic procedure).
If an endoscopic procedure is reported with only its base
procedure, do not pay separately for the base procedure.
Payment for the base procedure is included in the payment for the
other endoscopy.
4 = Subject to 25% reduction of the TC diagnostic imaging
(effective for services January 1, 2006 through June 30, 2010).
Subject to 50% reduction of the TC diagnostic imaging (effective
for services July 1, 2010 and after).
5 = Subject to 50% of the practice expense component for certain
therapy services (effective for services April 1, 2013 and after).
6 = Subject to 25% reduction of the second highest and
subsequent procedures to the TC of diagnostic cardiovascular
services, effective for services January 1, 2013, and thereafter.
```

7 = Subject to 25% reduction of the second highest and
subsequent procedures to the TC of diagnostic ophthalmology
services, effective for services January 1, 2013, and thereafter.

9 = Concept does not apply.


OPPS Indicator

```
A value of “1” means subject to OPPS payment cap
determination.
```
```
A value of “9” means not subject to OPPS payment cap
determination.
```

