# HMDA
Examining Home Mortgage Disclosure Act data through time.
You can find HMDA data here:
https://www.consumerfinance.gov/data-research/hmda/

Because HMDA data is aggregated at the census level, I have a script to aggregate it at the Zip Code level, using the HUD census-to-zip crosswalk. 

https://www.huduser.gov/portal/datasets/usps_crosswalk.html

2018+ HMDA data is slightly different from 2017 and before.
It is also crucial to map consistent census and zips, because both fields change through time. So a 2010-2020 Census mapper is required.
