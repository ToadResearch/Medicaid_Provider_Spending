## NATIONAL PHYSICIAN FEE SCHEDULE RELATIVE VALUE FILE CALENDAR YEAR 2026

**Contents** : This file contains information on services covered by the Medicare
Physician Fee Schedule (MPFS) in 2026. For more than 10,
physician services, the file contains the associated relative value units
(RVUs), a fee schedule status indicator, and various payment policy
indicators needed for payment adjustment (i.e., payment of assistant at
surgery, team surgery, bilateral surgery, etc.).

```
The Medicare physician fee schedule amounts are adjusted to reflect the variation in
practice costs from area to area. A geographic practice cost index (GPCI) has been
established for every Medicare payment locality for each of the three components of a
procedure’s relative value unit (i.e., the RVUs for work, practice expense, and
malpractice). The GPCIs are applied in the calculation of a fee schedule payment amount
by multiplying the RVU for each component times the GPCI for that component.
```
```
For informational purposes, changes from the previous year’s documentation file are in
bold font to facilitate their identification.
```
```
Section 121 of the Social Security Act Amendments of 1994 required
CMS to replace the existing charge-based practice expense relative value
units for all Medicare Physician Fee Schedule services with new
resource-based ones. The Balanced Budget Act of 1997 requires a four-
year transition from the existing charge-based system to the new
resource-based system beginning on January 1, 1999. In 2002 and
beyond, the practice expense relative value units are based entirely on
the resource-based system. Beginning with CY 2007, we will use a
bottom–up methodology for direct costs, use supplementary survey data
for indirect costs, and eliminate the nonphysician workpool in order to
calculate the practice expense RVUs. The nonphysician workpool was a
special method that has been used to calculate practice expense RVUs
for services with no physician work.
```
```
Under the resource-based system, we have developed practice expense
relative value units specific to the facility and non-facility settings.
Generally, under the resource-based system, the facility practice expense
RVUs will be used for services performed in inpatient or outpatient
hospital settings, emergency rooms, skilled nursing facilities, or
ambulatory surgical centers (ASCs). The non-facility practice expense
relative value units will be used for services furnished in all other settings.
We did not develop non-facility practice expense relative value units for
some services which, either by definition or in practice, are never (or
rarely) performed in a non-facility setting. For example, by definition, the
initial hospital care codes (CPT codes 99221-99223) are provided only in
the hospital inpatient setting. Also, many major surgical procedures with
a 90-day global period are almost always performed in the hospital
inpatient setting. These facility-only codes are identified by a “NA” in the
“NA Indicator” field.
```

The formula for 2026 physician fee schedule payment amount is as follows:

```
2026 Non-Facility Pricing Amount =
[(Work RVU * Work GPCI) +
(Non-Facility PE RVU * PE GPCI) +
(MP RVU * MP GPCI)] * Conversion Factor (CF)
```
```
2026 Facility Pricing Amount =
[(Work RVU * Work GPCI) +
(Facility PE RVU * PE GPCI) +
(MP RVU * MP GPCI)] * Conversion Factor
```
```
Certain therapy codes will receive a 50 percent reduction to the PE.
```
```
Section 5102(b) of the Deficit Reduction Act of 2005 requires a payment
cap on the technical component (TC) of certain diagnostic imaging
procedures and the TC portions of the global diagnostic imaging services.
This cap is based on the Outpatient Prospective Payment System
(OPPS) payment. To implement this provision, the physician fee
schedule amount is compared to the OPPS payment amount and the
lower amount is used in the formula below to calculate payment.
```
```
2026 OPPS Non-Facility Payment Amount =
[(Work RVU * Work GPCI) + (OPPS Non-Facility PE RVU * PE GPCI) +
(OPPS MP RVU * MP GPCI)] * Conversion Factor
```
```
2026 OPPS Facility Payment Amount =
[Work RVU * Work GPCI) + (OPPS Facility PE RVU * PE GPCI) +
(OPPS MP RVU * MP GPCI)] * Conversion Factor
```
We are attaching/including the 2026 locality-specific anesthesia conversion factors
(CFs) and they are calculated from the 2026 national anesthesia CF whose calculation is
described in the CY 2026 final physician fee schedule regulation.

The Medicare limiting charge is set by law at 115 percent of the payment amount for the
service furnished by the nonparticipating physician. However, the law sets the payment
amount for nonparticipating physicians at 95 percent of the payment amount for
participating physicians (i.e., the fee schedule amount). Calculating 95 percent of 115
percent of an amount is equivalent to multiplying the amount by a factor of 1.0925 (or
109.25 percent). Therefore, to calculate the Medicare limiting charge for a physician
service for a locality, multiply the fee schedule amount by a factor of 1.0925. The result is
the Medicare limiting charge for that service for that locality to which the fee schedule
amount applies.


**File Organization** : The file contains one record for each unique combination of procedure
code and modifier and is sorted in the above listed code
sequence.

```
Initial Source :
```
```
Data Set Name:
```
```
November 2025 Federal Register publication of the
Fee Schedule for Physicians' Services for CY 2026
```
```
RVU26A.Zip will contain six files:
```
```
(1) RVU26A (in Word (.docx) format) contains the file’s record layout and
file documentation;
(2) PPRRVU26 contains the RVUs and policy indicators associated with
the physician fee schedule in ASCII text (.txt), EXCEL (.xls) and
comma delimited format (.csv);
(3) GPCI2026 (in Excel (.xls), ASCII text (.prn) and comma delimited
(.csv) formats) provides each Geographic Practice Cost Index
(GPCI) component for each carrier/locality for 2026
(4) 26LOCCO (in Excel (.xls), ASCII text (.prn) and comma delimited
(.csv) formats) contains the locality/county crosswalk;
(5) ANES2026 (in Excel (.xls), ASCII text (.TXT) and comma delimited
(.csv) formats) contains the 2026 Anesthesia conversion factors; and
(6) OPPSCAP (in Excel (.xls), and comma delimited (.csv) formats)
contains the payment amounts after the application of the OPPS
based payment caps, except for carrier priced codes. For carrier
price codes, the field only contains the OPPS based payment caps.
Carrier prices cannot exceed the OPPS based payment caps.
```
**Length of Record:** 200 Characters

**Update Schedule** : This file will be updated on a periodic schedule to incorporate mid-year
changes. Updated 2026 files will be available on April 1, July 1 and
October 1. The following naming convention will be used to identify each:
RVU26A: January 2026 release
RVU26AR: January 2026 release

- Correction Notice (If Required)
RVU26B: April 2026 release
RVU26C: July 2026 release
RVU26D: October 2026 release

## NOTE:

- CPT codes and descriptions only are copyright 2026 American Medical Association.
    All rights reserved. Applicable FARS/DFARS apply.


- All dental codes copyright 2025/26 American Dental Association, all
    rights reserved.


## NATIONAL PHYSICIAN FEE SCHEDULE RELATIVE VALUE FILE

## CALENDAR YEAR 2026

## DATA ELEM ENT

## LOCATION

## COBOL

## PIC

## DESCRIPTION

## HEADER RECORD

Header Indicator
1 - 3
x(3)
Value “HDR”.

Filler 4 - 4 x(1)

Copyright Statement 5 - 50 x(46)

Filler 51 - 150 x(100)


## DATA RECORD

HCPCS Code
1 - 5
X(5)
CPT or Level 2 HCPCS number for the service.
NOTE: See copyright statement on cover sheet.

Modifier 6 - 7 X(2) For diagnostic tests, a blank in this field denotes the global service and
the following modifiers identify the components:
-- 26 = Professional component
--TC = Technical component
--For services other than those with a professional and/or technical
component, a blank will appear in this field with one exception: the
presence of CPT modifier -53 indicates that separate RVUs and a fee
schedule amount have been established for procedures which the
physician terminated before completion. This modifier is used only with
colonoscopy CPT codes 44388 and 45378, or with G0105 and G0121.
Any other codes billed with modifier -53 are subject to carrier medical
review and priced by individual consideration.
--53 = Discontinued Procedure - Under certain circumstances, the
physician may elect to terminate a surgical or diagnostic procedure.
Due to extenuating circumstances, or those that threaten the well being
of the patient, it may be necessary to indicate that a surgical or
diagnostic procedure was started but discontinued.

Description 8 - 57 X(50)

Status Code 58 - 58 X(1) Indicates whether the code is in the fee schedule and whether it is
separately payable if the service is covered. See Attachment A for
description of values. Only RVUs associated with status codes of "A",
"R", or "T", are used for Medicare payment.

Work RVU 60 - 65 999.99 Relative Value Unit (RVU) for the physician work in the service as


**Non-Facility Practice
Expense RVU**

## 67 - 72

## 999.

```
published in the Federal Register Fee Schedule for Physicians
Services for CY 2026
```
```
Relative Value Unit (RVU) for the resource-based practice expense for
the non-facility setting, as published in the Federal Register Fee
Schedule for Physicians Services for CY 2026
```
```
An “NA” in this field indicates that this procedure is rarely or never
performed in the non-facility setting.
```
```
Relative Value Unit (RVU) for the resource-based practice expense for
the facility setting, as published in the Federal Register Fee
Schedule for Physicians Services for CY 2026
```
```
An “NA” in this field indicates that this procedure is rarely or never
performed in the facility setting.
```
```
RVU for the malpractice expense for the service as published in the
Federal Register Fee Schedule for Physicians' Services for CY 2026
```
```
Sum of work, non-facility practice expense, and malpractice
expense RVUs.
```
```
Sum of work, facility practice expense, and malpractice expense
RVUs.
```
**Non-Facility NA Indicator**

**Facility Practice Expense
RVU**

**Facility NA Indicator**

**Malpractice RVU**

**Total Non-Facility RVUs**

**Total Facility RVUs**

## 73 - 74

## X(2)

## 76 - 81 999.

## 82 - 83

## X(2)

## 85 - 89

## 99.

## 91 - 96

## 999.

## 97 - 102

## 999.


**PC/TC Indicator 103 -** (^103) x(1) See Attachment A for description of values.
**Global Surgery 104 - 106** XXX Provides time frames that apply to each surgical procedure.
000=Endoscopic or minor procedure with related preoperative and
postoperative relative values on the day of the procedure only included
in the fee schedule payment amount; evaluation and management
services on the day of the procedure generally not payable.
010=Minor procedure with preoperative relative values on the day of the
procedure and postoperative relative values during a 10 day
postoperative period included in the fee schedule amount; evaluation
and management services on the day of the procedure and during the
10 - day postoperative period generally not payable.
090=Major surgery with a 1 - day preoperative period and 90 - day
postoperative period included in the fee schedule amount.
MMM=Maternity codes; usual global period does not apply.
XXX=The global concept does not apply to the code.
YYY=The carrier is to determine whether the global concept applies and
establishes postoperative period, if appropriate, at time of pricing.
ZZZ=The code is related to another service and is always included in
the global period of the other service.
**Preoperative Percentage 107 - 109** .99 Percentage for preoperative portion of global package.
**Intraoperative Percentage 110 - 112** .99 Percentage for intraoperative portion of global package, including
postoperative work in the hospital.
**Postoperative 113 - 115** .99 Percentage for postoperative portion of global package that is provided


**Percentage** in the office after discharge from the hospital.

```
Indicates applicable payment adjustment rule for multiple procedures:
0=No payment adjustment rules for multiple procedures apply. If
procedure is reported on the same day as another procedure, base the
payment on the lower of (a) the actual charge, or (b) the fee schedule
amount for the procedure.
```
```
1=Standard payment adjustment rules in effect before January 1, 1995
for multiple procedures apply. In the 1995 file, this indicator only
applies to codes with a status code of "D". If procedure is reported on
the same day as another procedure that has an indicator of 1, 2, or 3,
rank the procedures by fee schedule amount and apply the appropriate
reduction to this code (100%, 50%, 26%, and by report). Base the
payment on the lower of (a) the actual charge, or (b) the fee schedule
amount reduced by the appropriate percentage.
```
```
2=Standard payment adjustment rules for multiple procedures apply. If
procedure is reported on the same day as another procedure with an
indicator of 1, 2, or 3, rank the procedures by fee schedule amount and
apply the appropriate reduction to this code (100%, 50%, 50%, 50%,
50% and by report). Base the payment on the lower of (a) the actual
charge, or (b) the fee schedule amount reduced by the appropriate
percentage.
```
```
3=Special rules for multiple endoscopic procedures apply if procedure is
billed with another endoscopy in the same family (i.e., another
endoscopy that has the same base procedure). The base procedure for
each code with this indicator is identified in the Endobase field of this
file. Apply the multiple endoscopy rules to a family before ranking the
family with the other procedures performed on the same day (for
example, if multiple endoscopies in the same family are reported on the
same day as endoscopies in another family or on the same day as a
non-endoscopic procedure). If an endoscopic procedure is reported with
```
**Multiple Procedure
(Modifier 51)**

**116 -** (^116) x(1)


```
only its base procedure, do not pay separately for the base procedure.
Payment for the base procedure is included in the payment for the
other endoscopy.
```
4=Special rules for the technical component (TC) of diagnostic imaging
procedures apply if procedure is billed with another diagnostic imaging
procedure in the same family (per the diagnostic imaging family
indicator, below). If procedure is reported in the same session on the
same day as another procedure with the same family indicator, rank the
procedures by fee schedule amount for the TC. Pay 100% for the
highest priced procedure, and 50% for each subsequent procedure.
Base the payment for subsequent procedures on the lower of (a) the
actual charge, or (b) the fee schedule amount reduced by the
appropriate percentage. Subject to 50% reduction of the TC diagnostic
imaging (effective for services July 1, 2010 and after). Subject to 25%
reduction of the PC of diagnostic imaging (effective for services January
1, 2012 through December 31, 2016). Subject to 5% reduction of the
PC of diagnostic imaging (effective for services January 1, 2017 and
after).

```
5=Subject to 50% of the practice expense component for certain
therapy services.
```
```
6=Subject to 25% reduction of the second highest and subsequent
procedures to the TC of diagnostic cardiovascular services, effective for
services January 1, 2013, and thereafter.
```
```
7=Subject to 20% reduction of the second highest and subsequent
procedures to the TC of diagnostic ophthalmology services, effective for
services January 1, 2013, and thereafter.
```

9=Concept does not apply.

```
Bilateral Surgery
(Modifier 50)
```
**117 -** (^117) x(1) Indicates services subject to payment adjustment.
0=150% payment adjustment for bilateral procedures does not apply. If
procedure is reported with modifier -50 or with modifiers RT and LT,
base the payment for the two sides on the lower of: (a) the total actual
charge for both sides and (b) 100% of the fee schedule amount for a
single code. Example: The fee schedule amount for code XXXXX is
$125. The physician reports code XXXXX-LT with an actual charge of
$100 and XXXXX-RT with an actual charge of $100. Payment should
be based on the fee schedule amount ($125) since it is lower than the
total actual charges for the left and right sides ($200).
The bilateral adjustment is inappropriate for codes in this category (a)
because of physiology or anatomy, or (b) because the code description
specifically states that it is a unilateral procedure and there is an
existing code for the bilateral procedure.
1=150% payment adjustment for bilateral procedures applies. If the
code is billed with the bilateral modifier or is reported twice on the same
day by any other means (e.g., with RT and LT modifiers, or with a 2 in
the units field), base the payment for these codes when reported as
bilateral procedures on the lower of: (a) the total actual charge for both
sides or (b) 150% of the fee schedule amount for a single code. If the
code is reported as a bilateral procedure and is reported with other
procedure codes on the same day, apply the bilateral adjustment before
applying any multiple procedure rules.
2=150% payment adjustment does not apply. RVUs are already based
on the procedure being performed as a bilateral procedure. If the
procedure is reported with modifier - 50 or is reported twice on the same
day by any other means (e.g., with RT and LT modifiers or with a 2 in
the units field), base the payment for both sides on the lower of (a) the
total actual charge by the physician for both sides, or (b) 100% of the


fee schedule for a single code. Example: The fee schedule amount for
code YYYYY is $125. The physician reports code YYYYY-LT with an
actual charge of $100 and YYYYY-RT with an actual charge of $100.
Payment should be based on the fee schedule amount ($125) since it is
lower than the total actual charges for the left and right sides ($200).
The RVUs are based on a bilateral procedure because (a) the code
descriptor specifically states that the procedure is bilateral, (b) the code
descriptor states that the procedure may be performed either
unilaterally or bilaterally, or (c) the procedure is usually performed as a
bilateral procedure.

```
3=The usual payment adjustment for bilateral procedures does not
apply. If the procedure is reported with modifier -50 or is reported for
both sides on the same day by any other means (e.g., with RT and LT
modifiers or with a 2 in the units field), base the payment for each side
or organ or site of a paired organ on the lower of (a) the actual charge
for each side or (b) 100% of the fee schedule amount for each side. If
the procedure is reported as a bilateral procedure and with other
procedure codes on the same day, determine the fee schedule amount
for a bilateral procedure before applying any multiple procedure rules.
Services in this category are generally radiology procedures or other
diagnostic tests which are not subject to the special payment rules for
other bilateral surgeries.
```
```
9=Concept does not apply.
```
```
Assistant at Surgery 118 - 118 x(1) Indicates services where an assistant at surgery is never paid for per
Medicare Claims Manual.
0=Payment restriction for assistants at surgery applies to this procedure
unless supporting documentation is submitted to establish medical
necessity.
```
```
1=Statutory payment restriction for assistants at surgery applies to this
```

```
Co-surgeons
(Modifier 62)
```
**119 -** (^119) x(1)
**Team Surgery
(Modifier 66)
Pricing Indicator**

## 120 - 120

## 121 - 121

```
x(1)
```
```
x(1)
```
```
Filler 122 - 127 x(1)
```
```
Endoscopic Base Code 128 - 132 X(5)
```
```
procedure. Assistant at surgery may not be paid.
```
```
2=Payment restriction for assistants at surgery does not apply to this
procedure. Assistant at surgery may be paid.
```
```
9=Concept does not apply.
```
```
Indicates services for which two surgeons, each in a different specialty,
may be paid.
0=Co-surgeons not permitted for this procedure.
```
```
1=Co-surgeons could be paid, though supporting documentation is
required to establish the medical necessity of two surgeons for the
procedure.
```
```
2=Co-surgeons permitted and no documentation required if the two-
specialty requirement is met.
```
```
9=Concept does not apply.
```
```
Indicates services for which team surgeons may be paid.
0=Team surgeons not permitted for this procedure.1=Team surgeons
could be paid, though supporting documentation required to establish
medical necessity of a team; pay by report.
```
```
2=Team surgeons permitted; pay by report.
```
```
9=Concept does not apply.
```
```
1=QPP indicator applies to qualifying providers.
```
```
9=Concept does not apply.
```
```
Code which identifies an endoscopic base code for each code with a
```

multiple surgery indicator of 3.

```
Conversion Factor 133 - 140 999.9999 This is the multiplier that transforms relative values into payment
amounts. This conversion factor reflects the MEI update adjustment.
For 2002 and beyond, there is a single conversion factor for all services.
```
```
Physician Supervision of
Diagnostic Procedures
```
```
142 - 143 X(2) This field is for use in post payment review.
```
```
01 = Procedure must be performed under the general supervision of a
physician.
02 = Procedure must be performed under the direct supervision of a
physician.
03 = Procedure must be performed under the personal supervision of
physician.
```
```
(Diagnostic imaging procedures performed by a Registered
Radiologist Assistant (RRA) who is certified and registered by The
American Registry of Radiologic Technologists (ARRT) or a
Radiology Practitioner Assistant (RPA) who is certified by the
Certification Board for Radiology Practitioner Assistants (CBRPA),
and is authorized to furnish the procedure under state law, may be
performed under direct supervision.)
```
```
04 = Physician supervision policy does not apply when procedure is
furnished by a qualified, independent psychologist or a clinical
psychologist; otherwise must be performed under the general
supervision of a physician.
05 = Physician supervision policy does not apply when procedure is
furnished by a qualified audiologist; otherwise must be performed under
the general supervision of a physician.
06 = Procedure must be performed by a physician or a physical
Therapist (PT) who is certified by the American Board of Physical
Therapy Specialties (ABPTS) as a qualified electrophysiological clinical
specialist and is permitted to provide the procedure under State law.
21 = Procedure may be performed by a technician with certification
```

under general supervision of a physician; otherwise must be performed
under direct supervision of a physician.
22 = May be performed by a technician with on-line real-time contact
with physician.
66 = May be performed by a physician or by a physical therapist with
ABPTS certification and certification in this specific procedure.
6A= Supervision standards for level 66 apply; in addition, the PT with
ABPTS certification may supervise another PT, but only the PT with
ABPTS certification may bill.
77 = Procedure must be performed by a PT with ABPTS certification or
by a PT without certification under direct supervision of a physician, or by
a technician with certification under general supervision of a physician.
7A = Supervision standards for level 77 apply; in addition, the PT with
ABPTS certification may supervise another PT, but only the PT with
ABPTS certification may bill.
09 = Concept does not apply.


**Calculation Flag 144 - 144** X(1) As of July 1, 2014 the value for the Calculation Flag for all HCPCS is
“0”.

**Diagnostic Imaging Family
Indicator**

## 146 - 147

## X(2)

```
This field identifies the applicable diagnostic serrvice family for that
HCPCS codes with a multiple procedure indicator of ‘4’. For services
effective January 1, 2011 and after, family indicators 01 – 11 will not be
populated. The values are:
01=Ultrasound (Chest/Abdomen/Pelvis-Non-Obstetrical)
02=CT and CTA (Chest/Thorax/Abd/Pelvis)
03=CT and CTA (Head/Brain/Orbit/Maxillofacial/Neck)
04=MRI and MRA (Chest/Abd/Pelvis)
05=MRI and MRA (Head/Brain/Neck)
06=MRI and MRA (Spine)
07=CT (Spine)
08=MRI and MRA (Lower Extremities)
09=CT and CTA (Lower Extremities)
10=MR and MRI (Upper Extremities and Joints)
11=CT and CTA (Upper Extremities)
88 = Subject to the reduction of the TC diagnostic imaging (effective for
services January 1, 2011 and after). Subject to the reduction of the PC
diagnostic imaging (effective for services January 1, 2012, and after)
```

99=Concept does not apply

```
Non-Facility Practice
Expense Used for OPPS
Payment Amount
```
```
152 - 157 999.99 Theto the^ OPPS Medicare^ Payment Physician^ Amount Fee^ calculated Schedule^ using to determine^ these^ values appicability^ is^ compared of the
OPPS Imaging Cap mandated by Section 5102(b) of the Deficit
Reduction Act of 2005.
```
```
Facility Practice Expense
Used for OPPS Payment
Amount
```
```
159 - 164 999.99 Theto the^ OPPS Medicare^ Payment Physician^ Amount Fee^ calculated Schedule^ using to determine^ these^ values appicability^ is^ compared of the
OPPS Imaging Cap mandated by Section 5102(b) of the Deficit
Reduction Act of 2005.
```
```
Malpractice Used for OPPS
Payment Amount
```
```
1 999.99 Theto the Medicare Physician Fee Schedule to determine^ OPPS Payment^ Amount^ calculated using these values appicability^ is^ compared of the
OPPS Imaging Cap mandated by Section 5102(b) of the Deficit
Reduction Act of 2005.
```

## ATTACHMENT A

STATUS CODE A = Active Code. These codes are paid separately under the physician fee
schedule, if covered. There will be RVUs for codes with this status.
The presence of an "A" indicator does not mean that Medicare has
made a national coverage determination regarding the service; A/B
MACs (B) remain responsible for coverage decisions in the absence of
a national Medicare policy.

```
B = Bundled Code. Payment for covered services are always bundled into
payment for other services not specified. If RVUs are shown, they are
not used for Medicare payment. If these services are covered, payment
for them is subsumed by the payment for the services to which they are
incident. (An example is a telephone call from a hospital nurse
regarding care of a patient).
```
```
C = Contractors price the code. A/B MACs (B) will establish RVUs and
payment amounts for these services, generally on an individual case
basis following review of documentation such as an operative report.
```
```
D = Deleted Codes. These codes are deleted effective with the beginning
of the applicable year. These codes will not appear on the 2006 file as
the grace period for deleted codes is no longer applicable.
```
```
E = Excluded from Physician Fee Schedule by regulation. Thesecodes are
for items and/or services that CMS chose to exclude from the fee
schedule payment by regulation. No RVUs are shown, and no payment
may be made under the fee schedule for these codes. Payment for
them, when covered, generally continues under reasonable charge
procedures.
```
```
F = Deleted/Discontinued Codes. (Code not subject to a 90 day grace
period). These codes will not appear on the 2006 file as the grace
period for deleted codes is no longerapplicable.
```
```
G = Not valid for Medicare purposes. Medicare uses another code for
reporting of, and payment for, these services. (Code subject to a 90
day grace period.) These codes will not appear on the 2006 file as the
grace period for deleted codes is no longer applicable.
```
```
H = Deleted Modifier. This code had an associated TC and/or 26 modifier in
the previous year. For the current year, the TC or 26 component shown
for the code has been deleted, and the deleted component is shown
with a status code of "H". These codes will not appear on the 2006 file
as the grace period for deleted codes is no longer applicable.
```
```
I = Not valid for Medicare purposes. Medicare uses another code for
```

```
reporting of, and payment for, these services. (Code NOT subject to a
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
N = Non-covered Services. These services are not covered by Medicare.
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
R = Restricted Coverage. Special coverage instructions apply. If covered,
the service is contractor priced. (NOTE: The majority of codes to
which this indicator will be assigned are the alpha-numeric dental
codes, which begin with "D". We are assigning the indicator to a
limited number of CPT codes which represent services that are covered
only in unusual circumstances.)
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
codes, and no payment may be made under the physician fee schedule.
(Examples are ambulance services and clinical diagnostic laboratory
services.)


PC/TC INDICATOR 0 = Physician Service Codes--Identifies codes that describe
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
physician work, practice expense, and malpracticeexpense.
The total RVUs for codes reported with a TC modifier include
values for practice expense and malpractice expense only. The
total RVUs for codes reported without a modifier include values
for physician work, practice expense, and malpractice expense.
```
```
2 = Professional Component Only Codes--This indicator identifies
stand-alone codes that describe the physician work portion of
selected diagnostic tests for which there is an associated code
that describes the technical component of the diagnostic test
only and another associated code that describes the global test.
An example of a professional component only code is CPT code
93010 --Electrocardiogram ; Interpretation and Report.
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
technical component only code is CPT code 93005 --
Electrocardiogram ; Tracing Only, without interpretation and
report. It also identifies codes that are covered only as
diagnostic tests and therefore do not have a related professional
code. Modifiers 26 and TC cannot be used with these codes.
The total RVUs for technical component only codes include
values for practice expense and malpractice expense only.
```
```
4 = Global Test Only Codes--This indicator identifies stand-alone
codes that describe selected diagnostic tests for which there are
associated codes that describe (a) the professional component
```

```
of the test only, and (b) the technical component of the test only.
Modifiers 26 and TC cannot be used with these codes. The total
RVUs for global procedure only codes include values for
physician work, practice expense, and malpractice expense.
The total RVUs for global procedure only codes equals the sum
of the total RVUs for the professional and technical components
only codes combined.
```
5 = Incident To Codes--This indicator identifies codes that describe
services covered incident to a physician's service when they are
provided by auxiliary personnel employed by the physician and
working under his or her direct personal supervision. Payment
may not be made by A/B MACs (B) for these services when
they are provided to hospital inpatients or patients in a hospital
outpatient department. Modifiers 26 and TC cannot be used with
these codes.

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


8 = Physician interpretation codes: This indicator identifies
the professional component of clinical laboratory codes
for which separate payment may be made only if the
physician interprets an abnormal smear for hospital
inpatient. This applies to CPT codes 85060. No TC
billing is recognized because payment for the
underlying clinical laboratory test is made to the
hospital, generally through the PPS rate.

```
No payment is recognized for CPT codes 85060
furnished to hospital outpatients or non-hospital
patients. The physician interpretation is paid through
the clinical laboratory fee schedule payment for the
clinical laboratory test.
```
```
9 = Not Applicable--Concept of a professional/technical
component does not apply
```

