import io
import argparse
from datetime import date
from datetime import timedelta
import requests
import pandas as pd
import json
import os
import time
import gzip
import shutil

# companies to run through 
# credentials stored in external file
'''
companies = [
    {
        'company' : 'bayer',
        'REFRESH_TOKEN' : "Atzr|IwEBIEIxxx",
        'CLIENT_ID' : "amzn1.application-oa2-client.xxx",
        'CLIENT_SECRET' : "amzn1.oa2-cs.v1.xxx"
        },
    {
        'company':'gloryfeel',
        'REFRESH_TOKEN' : "Atzr|IwEBIEIxxx",
        'CLIENT_ID' : "amzn1.application-oa2-client.xxx",
        'CLIENT_SECRET' : "amzn1.oa2-cs.v1.xxx"
    }
]

'''
with open('../amazon_ads_credentials.json', 'r') as fp:
    companies = json.load(fp)

# report definitions with report windows and specifications
reports = [
    {
    "adProduct": "SPONSORED_PRODUCTS",
    "reportTypeId" : "spCampaigns",
    "localName":"campaign_details",
    "groupBy":["campaign","adGroup"],
    "columns":["impressions","clicks","cost","purchases1d","purchases7d","purchases14d","purchases30d","purchasesSameSku1d","purchasesSameSku7d","purchasesSameSku14d","purchasesSameSku30d","unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d","sales1d","sales7d","sales14d","sales30d","attributedSalesSameSku1d","attributedSalesSameSku7d","attributedSalesSameSku14d","attributedSalesSameSku30d","unitsSoldSameSku1d","unitsSoldSameSku7d","unitsSoldSameSku14d","unitsSoldSameSku30d","date","campaignBiddingStrategy","costPerClick","clickThroughRate","spend","campaignName","campaignId","campaignStatus","campaignBudgetAmount","campaignBudgetType","campaignRuleBasedBudgetAmount","campaignApplicableBudgetRuleId","campaignApplicableBudgetRuleName","campaignBudgetCurrencyCode","adGroupName","adGroupId","adStatus"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":95
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbCampaigns",
    "localName":"campaign_details",
    "groupBy":["campaign"],
    "columns":["addToCart","addToCartClicks","addToCartRate","brandedSearches","brandedSearchesClicks","campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","campaignId","campaignName","campaignStatus","clicks","cost","costType","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","impressions","newToBrandDetailPageViewRate","newToBrandDetailPageViews","newToBrandDetailPageViewsClicks","newToBrandECPDetailPageView","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandPurchasesPercentage","newToBrandPurchasesRate","newToBrandSales","newToBrandSalesClicks","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldClicks","newToBrandUnitsSoldPercentage","purchases","purchasesClicks","purchasesPromoted","sales","salesClicks","salesPromoted","topOfSearchImpressionShare","unitsSold","unitsSoldClicks","video5SecondViewRate","video5SecondViews","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewableImpressions","viewClickThroughRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":60
},
{
    "adProduct": "SPONSORED_DISPLAY",
    "reportTypeId" : "sdCampaigns",
    "localName":"campaign_details",
    "groupBy":["campaign"],
    "columns":["addToCart","addToCartClicks","addToCartRate","addToCartViews","brandedSearches","brandedSearchesClicks","brandedSearchesViews","brandedSearchRate","campaignBudgetCurrencyCode","campaignId","campaignName","clicks","cost","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","eCPBrandSearch","impressions","impressionsViews","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandSalesClicks","newToBrandUnitsSold","newToBrandUnitsSoldClicks","purchases","purchasesClicks","purchasesPromotedClicks","sales","salesClicks","salesPromotedClicks","unitsSold","unitsSoldClicks","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewClickThroughRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":65
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbAdGroup",
    "localName":"ad_groups",
    "groupBy":["adGroup"],
    "columns":["addToCart","addToCartClicks","addToCartRate","adGroupId","adGroupName","adStatus","brandedSearches","brandedSearchesClicks","campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","campaignId","campaignName","campaignStatus","clicks","cost","costType","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","impressions","newToBrandDetailPageViewRate","newToBrandDetailPageViews","newToBrandDetailPageViewsClicks","newToBrandECPDetailPageView","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandPurchasesPercentage","newToBrandPurchasesRate","newToBrandSales","newToBrandSalesClicks","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldClicks","newToBrandUnitsSoldPercentage","purchases","purchasesClicks","purchasesPromoted","sales","salesClicks","salesPromoted","unitsSold","unitsSoldClicks","video5SecondViewRate","video5SecondViews","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":60
},
{
    "adProduct": "SPONSORED_DISPLAY",
    "reportTypeId" : "sdAdGroup",
    "localName":"ad_groups",
    "groupBy":["adGroup"],
    "columns":["addToCart","addToCartClicks","addToCartRate","addToCartViews","adGroupId","adGroupName","bidOptimization","brandedSearches","brandedSearchesClicks","brandedSearchesViews","brandedSearchRate","campaignBudgetCurrencyCode","campaignId","campaignName","clicks","cost","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","eCPBrandSearch","impressions","impressionsViews","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandSales","newToBrandSalesClicks","newToBrandUnitsSold","newToBrandUnitsSoldClicks","purchases","purchasesClicks","purchasesPromotedClicks","sales","salesClicks","salesPromotedClicks","unitsSold","unitsSoldClicks","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewClickThroughRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":65
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbCampaignPlacement",
    "localName":"campaign_placement",
    "groupBy":["campaignPlacement"],
    "columns":["addToCart","addToCartClicks","addToCartRate","brandedSearches","brandedSearchesClicks","campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","campaignId","campaignName","campaignStatus","clicks","cost","costType","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","impressions","newToBrandDetailPageViewRate","newToBrandDetailPageViews","newToBrandDetailPageViewsClicks","newToBrandECPDetailPageView","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandPurchasesPercentage","newToBrandPurchasesRate","newToBrandSales","newToBrandSalesClicks","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldClicks","newToBrandUnitsSoldPercentage","purchases","purchasesClicks","purchasesPromoted","sales","salesClicks","salesPromoted","unitsSold","unitsSoldClicks","video5SecondViewRate","video5SecondViews","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewableImpressions","viewClickThroughRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":60
},
{
    "adProduct": "SPONSORED_PRODUCTS",
    "reportTypeId" : "spTargeting",
    "localName":"keywords",
    "groupBy":["targeting"],
    "columns":["impressions","clicks","costPerClick","clickThroughRate","cost","purchases1d","purchases7d","purchases14d","purchases30d","purchasesSameSku1d","purchasesSameSku7d","purchasesSameSku14d","purchasesSameSku30d","unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d","sales1d","sales7d","sales14d","sales30d","attributedSalesSameSku1d","attributedSalesSameSku7d","attributedSalesSameSku14d","attributedSalesSameSku30d","unitsSoldSameSku1d","unitsSoldSameSku7d","unitsSoldSameSku14d","unitsSoldSameSku30d","kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d","salesOtherSku7d","unitsSoldOtherSku7d","acosClicks7d","acosClicks14d","roasClicks7d","roasClicks14d","keywordId","keyword","campaignBudgetCurrencyCode","date","portfolioId","campaignName","campaignId","campaignBudgetType","campaignBudgetAmount","campaignStatus","keywordBid","adGroupName","adGroupId","keywordType","matchType","targeting","adKeywordStatus"],
    "timeUnit": "DAILY",
    "filters":[{"field":"keywordType","values":["BROAD", "PHRASE", "EXACT"]}],
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":95
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbTargeting",
    "localName":"keywords",
    "groupBy":["targeting"],
    "columns":["addToCart","addToCartClicks","addToCartRate","adGroupId","adGroupName","brandedSearches","brandedSearchesClicks","campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","campaignId","campaignName","campaignStatus","clicks","cost","costType","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","impressions","keywordBid","keywordId","adKeywordStatus","keywordText","keywordType","matchType","newToBrandDetailPageViewRate","newToBrandDetailPageViews","newToBrandDetailPageViewsClicks","newToBrandECPDetailPageView","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandPurchasesPercentage","newToBrandPurchasesRate","newToBrandSales","newToBrandSalesClicks","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldClicks","newToBrandUnitsSoldPercentage","purchases","purchasesClicks","purchasesPromoted","sales","salesClicks","salesPromoted","targetingExpression","targetingId","targetingText","targetingType","topOfSearchImpressionShare"],
    "timeUnit": "DAILY",
    "filters":[{"field":"keywordType","values":["BROAD", "PHRASE", "EXACT"]}],
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":60
},
{
    "adProduct": "SPONSORED_DISPLAY",
    "reportTypeId" : "sdTargeting",
    "localName":"keywords",
    "groupBy":["targeting"],
    "columns":["addToCart","addToCartClicks","addToCartRate","addToCartViews","adGroupId","adGroupName","brandedSearches","brandedSearchesClicks","brandedSearchesViews","brandedSearchRate","campaignBudgetCurrencyCode","campaignId","campaignName","clicks","cost","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","eCPBrandSearch","impressions","impressionsViews","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandSales","newToBrandSalesClicks","newToBrandUnitsSold","newToBrandUnitsSoldClicks","purchases","purchasesClicks","purchasesPromotedClicks","sales","salesClicks","salesPromotedClicks","targetingExpression","targetingId","targetingText","unitsSold","unitsSoldClicks","videoCompleteViews"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":65
},
{
    "adProduct": "SPONSORED_PRODUCTS",
    "reportTypeId" : "spSearchTerm",
    "localName":"search_terms",
    "groupBy":["searchTerm"],
    "columns":["keywordId","keyword","impressions","clicks","costPerClick","clickThroughRate","cost","purchases1d","purchases7d","purchases14d","purchases30d","purchasesSameSku1d","purchasesSameSku7d","purchasesSameSku14d","purchasesSameSku30d","unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d","sales1d","sales7d","sales14d","sales30d","attributedSalesSameSku1d","attributedSalesSameSku7d","attributedSalesSameSku14d","attributedSalesSameSku30d","unitsSoldSameSku1d","unitsSoldSameSku7d","unitsSoldSameSku14d","unitsSoldSameSku30d","kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d","salesOtherSku7d","unitsSoldOtherSku7d","acosClicks7d","acosClicks14d","roasClicks7d","roasClicks14d","campaignBudgetCurrencyCode","date","portfolioId","campaignName","campaignId","campaignBudgetType","campaignBudgetAmount","campaignStatus","keywordBid","adGroupName","adGroupId","keywordType","matchType","targeting","adKeywordStatus"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":95
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbSearchTerm",
    "localName":"search_terms",
    "groupBy":["searchTerm"],
    "columns":["adGroupId","adGroupName","campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","campaignId","campaignName","campaignStatus","clicks","cost","costType","date","impressions","keywordBid","keywordId","keywordText","matchType","purchases","purchasesClicks","sales","salesClicks","searchTerm","unitsSold","video5SecondViewRate","video5SecondViews","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewableImpressions","viewClickThroughRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":60
},
{
    "adProduct": "SPONSORED_PRODUCTS",
    "reportTypeId" : "spAdvertisedProduct",
    "localName":"advertised_products",
    "groupBy":["advertiser"],
    "columns":["date","campaignName","campaignId","adGroupName","adGroupId","adId","portfolioId","impressions","clicks","costPerClick","clickThroughRate","cost","spend","campaignBudgetCurrencyCode","campaignBudgetAmount","campaignBudgetType","campaignStatus","advertisedAsin","advertisedSku","purchases1d","purchases7d","purchases14d","purchases30d","purchasesSameSku1d","purchasesSameSku7d","purchasesSameSku14d","purchasesSameSku30d","unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d","sales1d","sales7d","sales14d","sales30d","attributedSalesSameSku1d","attributedSalesSameSku7d","attributedSalesSameSku14d","attributedSalesSameSku30d","salesOtherSku7d","unitsSoldSameSku1d","unitsSoldSameSku7d","unitsSoldSameSku14d","unitsSoldSameSku30d","unitsSoldOtherSku7d","kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d","acosClicks7d","acosClicks14d","roasClicks7d","roasClicks14d"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":95
},
{
    "adProduct": "SPONSORED_DISPLAY",
    "reportTypeId" : "sdAdvertisedProduct",
    "localName":"advertised_products",
    "groupBy":["advertiser"],
    "columns":["addToCart","addToCartClicks","addToCartRate","addToCartViews","adGroupId","adGroupName","adId","bidOptimization","brandedSearches","brandedSearchesClicks","brandedSearchesViews","brandedSearchRate","campaignBudgetCurrencyCode","campaignId","campaignName","clicks","cost","cumulativeReach","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","eCPBrandSearch","impressions","impressionsFrequencyAverage","impressionsViews","newToBrandDetailPageViewClicks","newToBrandDetailPageViewRate","newToBrandDetailPageViews","newToBrandDetailPageViewViews","newToBrandECPDetailPageView","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandSales","newToBrandSalesClicks","newToBrandUnitsSold","newToBrandUnitsSoldClicks","promotedAsin","promotedSku","purchases","purchasesClicks","purchasesPromotedClicks","sales","salesClicks","salesPromotedClicks","unitsSold","unitsSoldClicks","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewClickThroughRate"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":65
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbAds",
    "localName":"ads",
    "groupBy":["ads"],
    "columns":["addToCart","addToCartClicks","addToCartRate","adGroupId","adGroupName","adId","brandedSearches","brandedSearchesClicks","campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","campaignId","campaignName","campaignStatus","clicks","cost","costType","date","detailPageViews","detailPageViewsClicks","eCPAddToCart","impressions","newToBrandDetailPageViewRate","newToBrandDetailPageViews","newToBrandDetailPageViewsClicks","newToBrandECPDetailPageView","newToBrandPurchases","newToBrandPurchasesClicks","newToBrandPurchasesPercentage","newToBrandPurchasesRate","newToBrandSales","newToBrandSalesClicks","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldClicks","newToBrandUnitsSoldPercentage","purchases","purchasesClicks","purchasesPromoted","sales","salesClicks","salesPromoted","unitsSold","unitsSoldClicks","video5SecondViewRate","video5SecondViews","videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate","viewableImpressions"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":60
},
{
    "adProduct": "SPONSORED_PRODUCTS",
    "reportTypeId" : "spPurchasedProduct",
    "localName":"purchased_products",
    "groupBy":["asin"],
    "columns":["date","portfolioId","campaignName","campaignId","adGroupName","adGroupId","keywordId","keyword","keywordType","advertisedAsin","purchasedAsin","advertisedSku","campaignBudgetCurrencyCode","matchType","unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d","sales1d","sales7d","sales14d","sales30d","purchases1d","purchases7d","purchases14d","purchases30d","unitsSoldOtherSku1d","unitsSoldOtherSku7d","unitsSoldOtherSku14d","unitsSoldOtherSku30d","salesOtherSku1d","salesOtherSku7d","salesOtherSku14d","salesOtherSku30d","purchasesOtherSku1d","purchasesOtherSku7d","purchasesOtherSku14d","purchasesOtherSku30d"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":95
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbPurchasedProduct",
    "localName":"purchased_products",
    "groupBy":["purchasedAsin"],
    "columns":["date","campaignBudgetCurrencyCode","campaignName","adGroupName","attributionType","purchasedAsin","productName","productCategory","sales14d","orders14d","unitsSold14d","newToBrandSales14d","newToBrandPurchases14d","newToBrandUnitsSold14d","newToBrandSalesPercentage14d","newToBrandPurchasesPercentage14d","newToBrandUnitsSoldPercentage14d"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 731,
    "retention":731
},
{
    "adProduct": "SPONSORED_DISPLAY",
    "reportTypeId" : "sdPurchasedProduct",
    "localName":"purchased_products",
    "groupBy":["asin"],
    "columns":["adGroupId","adGroupName","asinBrandHalo","campaignBudgetCurrencyCode","campaignId","campaignName","conversionsBrandHalo","conversionsBrandHaloClicks","date","promotedAsin","promotedSku","salesBrandHalo","salesBrandHaloClicks","unitsSoldBrandHalo","unitsSoldBrandHaloClicks"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 31,
    "retention":65
},
{
    "adProduct": "SPONSORED_PRODUCTS",
    "reportTypeId" : "spGrossAndInvalids",
    "localName":"invalids",
    "groupBy":["campaign"],
    "columns":["campaignName", "impressions","clicks","grossImpressions","grossClickThroughs","invalidClickThroughs","invalidClickThroughRate","date"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 30,
    "retention":95
},
{
    "adProduct": "SPONSORED_BRANDS",
    "reportTypeId" : "sbGrossAndInvalids",
    "localName":"invalids",
    "groupBy":["campaign"],
    "columns":["campaignName", "impressions","clicks","grossImpressions","grossClickThroughs","invalidClickThroughs","invalidClickThroughRate","date"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 30,
    "retention":95
},
{
    "adProduct": "SPONSORED_DISPLAY",
    "reportTypeId" : "sdGrossAndInvalids",
    "localName":"invalids",
    "groupBy":["campaign"],
    "columns":["campaignName", "impressions","clicks","grossImpressions","grossClickThroughs","invalidClickThroughs","invalidClickThroughRate","date"],
    "timeUnit": "DAILY",
    "format": "GZIP_JSON",
    "dateRange": 30,
    "retention":95
}
]

# python variables
'''
Usage 

python amazon_ads_v3.py --company xxx --market xxx -specifc_date YYYY-MM-DD --specific_report xxx

Scheduled job today has multiple lines 
One for the last 31 days of data for specific company and market - this way we are sure each company and market will be downloaded for sure.
bayer ES
bayer IT
bayer FR
...
gloryfeel DE
gloryfeel IT
...


$1 company scope - bayer or gloryfeel or ALL
$2 market - specific market code e.g. DE or UK or ALL
$3 period - full reload of available dates - either date or ALL
$4 report type to download - specific reportTypeId like sbAds or ALL
'''

'''
structure of the app
get everything for company (including country list where company operates)
    based on market(country spec) either run market or through whole list
        check defined date to download data if it will loop or just run once
            check running report name and run it according previous conditions
                if payload succesfull save to appropriate place localy and on GCS
'''

def run_argument_parser():
    # Create the parser
    parser = argparse.ArgumentParser(description="Running Amazon Ads API calls for specific company, market, country or report.")

    # Add arguments
    parser.add_argument("--company",        help="Specific company name (bayer, gloryfeel, ALL).",            type=str, default="ALL")
    parser.add_argument("--market",         help="Market name (DE, UK, ES, etc... or ALL).",                  type=str, default="ALL")
    parser.add_argument("--specific_date",  help="Specific date for running the code - 2023-11-30 or ALL.",   type=str, default="ALL")
    parser.add_argument("--specific_report",help="Specific report (like spCampaigns) - or put ALL.",          type=str, default="ALL")

    # Parse arguments
    args = parser.parse_args()

    # Implement your application logic here using the parameters
    # For example: print the parameters
    print(f"Value for company:  {args.company}")
    print(f"Value for market:   {args.market}")
    print(f"Value for date:     {args.specific_date}")
    print(f"Value for report:   {args.specific_report}")
    
    return args

def get_running_dates(running_date):
    if running_date != 'ALL':
        start_date = date.fromisoformat(running_date)
        # check if the date is first day of month
        is_first_day = start_date.day == 1
        start_date = str(start_date)[0:10]
    else:
        start_date = date.today()-timedelta(days=1)
        # check if the date is first day of month
        is_first_day = start_date.day == 1
        start_date = str(start_date)
    
    return start_date # actually this is end date

def get_company(running_company):
    # in case of specific name get all credentials for a company
    if running_company != "ALL":
        # run through the list of companies and credentials
        for id_element in range(0,len(companies)):
            # check if company name is the same as in company listing
            if(running_company==companies[id_element]['company']):
                # get all credentials from company identification (refresh tokens, clientid and secret)
                company_name = companies[id_element]
    #else:
    #    company_name = companies
    # send it back
    return company_name

# get access token
def get_access_token(CLIENT_ID,CLIENT_SECRET,REFRESH_TOKEN):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    }
    
    data = {
      'grant_type': 'refresh_token',
      'client_id': CLIENT_ID,
      'refresh_token': REFRESH_TOKEN,
      'client_secret': CLIENT_SECRET
    }
    
    response = requests.post('https://api.amazon.com/auth/o2/token', headers=headers, data=data)
    r_json = response.json()
    return r_json["access_token"]

# get country profiles using credentials
def get_company_profiles(credential_list):
    # get access token 
    CLIENT_ID = credential_list['CLIENT_ID']
    CLIENT_SECRET = credential_list['CLIENT_SECRET']
    REFRESH_TOKEN = credential_list['REFRESH_TOKEN']
    ACCESS_TOKEN = get_access_token(CLIENT_ID,CLIENT_SECRET,REFRESH_TOKEN)
    
    # get profiles
    headers = {
        'Amazon-Advertising-API-ClientId':CLIENT_ID,
        'Authorization': ACCESS_TOKEN
    }
    response = requests.get("https://advertising-api-eu.amazon.com/v2/profiles", headers=headers)

    # dataframe from json
    profile_ids_df = pd.json_normalize(response.json())
    print('These are the profile IDs...')
    # stick to Sponsored Ads and skip agency or DSP account
    results_profile_ids = profile_ids_df[profile_ids_df['accountInfo.type']!="agency"]
    results_profile_ids = results_profile_ids.reset_index(drop=True)
    #results_profile_ids = profile_ids_df[profile_ids_df['accountInfo.type']=="vendor"]
    print(results_profile_ids)
    
    return results_profile_ids, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN
# SP, SB, SD campaign list
# Sponsored brands list of campaigns
def get_sb_campaigns_list(amazon_ads_profile_id,country,currency,CLIENT_ID,ACCESS_TOKEN):
    headers = {
        'Amazon-Advertising-API-ClientId':  CLIENT_ID,
        'Amazon-Advertising-API-Scope':     str(amazon_ads_profile_id), # loop through profiles
        'Authorization':    'Bearer '       +ACCESS_TOKEN,
        'Accept':           'application/vnd.sbcampaignresource.v4+json',
        'Accept-Encoding':  'gzip, deflate, br'
    }
    data = {
  "stateFilter": {
    "include": ["ENABLED","PAUSED"],
    "maxResults":1000
  }
}
    
    # perform the request
    small_result = requests.post('https://advertising-api-eu.amazon.com/sb/v4/campaigns/list',headers=headers,json=data)
    payload = json.loads(small_result.text)
    campaign_list = pd.json_normalize(json.loads(small_result.text)['campaigns'], max_level=4)
    
    while 'nextToken' in payload:
        # add next token
        data['nextToken']=payload['nextToken']
        small_result = requests.post('https://advertising-api-eu.amazon.com/sb/v4/campaigns/list',headers=headers,json=data)
        payload = json.loads(small_result.text)
        campaign_list = pd.concat([campaign_list, pd.json_normalize(json.loads(small_result.text)['campaigns'], max_level=4)], ignore_index=True)
    # add country name to the list
    campaign_list['country']=country
    campaign_list['currency']=currency
    campaign_list['profileId']=amazon_ads_profile_id
    
    return campaign_list
# Sponsored display list of campaigns
def get_sd_campaigns_list(amazon_ads_profile_id,country,currency,CLIENT_ID,ACCESS_TOKEN):
    headers = {
        'Amazon-Advertising-API-ClientId':  CLIENT_ID,
        'Amazon-Advertising-API-Scope':     str(amazon_ads_profile_id), # loop through profiles
        'Authorization':    'Bearer '       +ACCESS_TOKEN,
        'Accept-Encoding':  'gzip, deflate, br',
        'Accept':'*/*'
    }
    # perform the request
    small_result = requests.get('https://advertising-api-eu.amazon.com/sd/campaigns',headers=headers)
    payload = json.loads(small_result.text)
    campaign_list = pd.json_normalize(json.loads(small_result.text), max_level=4)
    
    campaign_list['country']=country
    campaign_list['currency']=currency
    campaign_list['profileId']=amazon_ads_profile_id
    
    return campaign_list
# Sponsored products list of campaigns
def get_sp_campaigns_list(amazon_ads_profile_id,country,currency,CLIENT_ID,ACCESS_TOKEN):
    headers = {
        'Amazon-Advertising-API-ClientId':  CLIENT_ID,
        'Amazon-Advertising-API-Scope':     str(amazon_ads_profile_id), # loop through profiles
        'Authorization':    'Bearer '       +ACCESS_TOKEN,
        'Accept':           'application/vnd.spCampaign.v3+json',
        'Content-Type':     'application/vnd.spCampaign.v3+json',
        'Accept-Encoding':  'gzip, deflate, br'
    }
    
    data = {
    'stateFilter': {
        'include': ['ENABLED','PAUSED']
        },
    'maxResults': 1000
    }
    
    # perform the request
    small_result = requests.post('https://advertising-api-eu.amazon.com/sp/campaigns/list',headers=headers,json=data)
    payload = json.loads(small_result.text)
    campaign_list = pd.json_normalize(json.loads(small_result.text)['campaigns'], max_level=4)
    
    while 'nextToken' in payload:
        # add next token
        data['nextToken']=payload['nextToken']
        small_result = requests.post('https://advertising-api-eu.amazon.com/sp/campaigns/list',headers=headers,json=data)
        payload = json.loads(small_result.text)
        campaign_list = pd.concat([campaign_list, pd.json_normalize(json.loads(small_result.text)['campaigns'], max_level=4)], ignore_index=True)
    # add country name to the list
    campaign_list['country']=country
    campaign_list['currency']=currency
    campaign_list['profileId']=amazon_ads_profile_id
    
    return campaign_list
# campaing size and saving
def check_size_and_save(local_filename,todays_date,company,campaign_type,gcs_name):
    #size_of_json_file = os.stat('./advertising/'+local_filename).st_size
    size_of_json_file = os.stat(local_path + local_filename).st_size
    
    if size_of_json_file>400:
        # transform original JSON to BigQuery structure
        # transform_to_ndjson(output_file, new_output_file)
        print('Output is good... Lets save on cloud')
        cloud_output_file = gs_path+'/'+campaign_type+'/'+gcs_name+'/dt='+ todays_date + '/source=' + company + '/' + local_filename
        save_local_to_gcs(local_path + local_filename, cloud_output_file)
    
    # move processed file to cold storage
    os.rename(local_path+local_filename, local_path+cold_storage_path+local_filename)

# process all campaigns information
def get_all_campaigns(profiles, start_date, company, todays_date, CLIENT_ID,ACCESS_TOKEN):
    
    # initialize DataFrames for campaign data
    sp_all_campaigns_info = pd.DataFrame()
    sb_all_campaigns_info = pd.DataFrame()
    sd_all_campaigns_info = pd.DataFrame()
    
    for index in range(0,len(profiles)):
        print('Running currently country '+profiles['countryCode'][index]+' and profile ID - '+str(profiles['profileId'][index]))
        sp_campaigns_payload = get_sp_campaigns_list(str(profiles['profileId'][index]),profiles['countryCode'][index],profiles['currencyCode'][index], CLIENT_ID,ACCESS_TOKEN)
        sb_campaigns_payload = get_sb_campaigns_list(str(profiles['profileId'][index]),profiles['countryCode'][index],profiles['currencyCode'][index], CLIENT_ID,ACCESS_TOKEN)
        sd_campaigns_payload = get_sd_campaigns_list(str(profiles['profileId'][index]),profiles['countryCode'][index],profiles['currencyCode'][index], CLIENT_ID,ACCESS_TOKEN)    
    
        sp_all_campaigns_info = pd.concat(objs=[sp_all_campaigns_info,sp_campaigns_payload], ignore_index=True)
        sb_all_campaigns_info = pd.concat(objs=[sb_all_campaigns_info,sb_campaigns_payload], ignore_index=True)
        sd_all_campaigns_info = pd.concat(objs=[sd_all_campaigns_info,sd_campaigns_payload], ignore_index=True)
    
    # prepare file name for local and remote storage
    sp_all_campaigns_info_filename = 'all_sp_campaigns_information_'+start_date+'_'+company+'.json'
    sb_all_campaigns_info_filename = 'all_sb_campaigns_information_'+start_date+'_'+company+'.json'
    sd_all_campaigns_info_filename = 'all_sd_campaigns_information_'+start_date+'_'+company+'.json'
    
    # prepare columns
    sp_all_campaigns_info.rename(columns=lambda x: x.replace(".", "_"), inplace=True)
    sb_all_campaigns_info.rename(columns=lambda x: x.replace(".", "_"), inplace=True)
    sd_all_campaigns_info.rename(columns=lambda x: x.replace(".", "_"), inplace=True)
    
    # save to JSON files
    sp_all_campaigns_info.to_json(local_path + sp_all_campaigns_info_filename, orient="records", lines=True)
    sb_all_campaigns_info.to_json(local_path + sb_all_campaigns_info_filename, orient="records", lines=True)
    sd_all_campaigns_info.to_json(local_path + sd_all_campaigns_info_filename, orient="records", lines=True)
    
    print('Managed to get all campaigns information...')
    # prepare file name for local and remote storage
    check_size_and_save(sp_all_campaigns_info_filename,todays_date,company,'sponsored_products','campaigns')
    check_size_and_save(sb_all_campaigns_info_filename,todays_date,company,'sponsored_brands','campaigns')
    check_size_and_save(sd_all_campaigns_info_filename,todays_date,company,'sponsored_display','campaigns')
#  save local file to global place
def save_local_to_gcs(local_input_file, cloud_output_file):
        time.sleep(1)
        # store file from local to GCS
        command_store_to_gcs="gsutil cp "+local_input_file+" " + cloud_output_file
        os.system(command_store_to_gcs)
        print("All cloud files saved here!")


# downloading the specific report to local storage
def waitForReportData(reportId, headers,running_profile_id,country,running_report,end_date,company,campaign_type,report_type):
    
    # request the data for specific report
    getReportResponse = requests.get('https://advertising-api-eu.amazon.com/reporting/reports/'+reportId,headers=headers)
    # what is the payload?
    payload = pd.json_normalize(getReportResponse.json())
    
    default_sleep_time = 10
    
    while payload['status'][0] != 'COMPLETED':
        print("Report status is: %s. Sleeping for %d seconds..." % (payload['status'][0],default_sleep_time))
        # increase by 10 seconds every cycle
        default_sleep_time=default_sleep_time+10
        time.sleep(default_sleep_time)
        getReportResponse = requests.get('https://advertising-api-eu.amazon.com/reporting/reports/'+reportId,headers=headers)
        payload = pd.json_normalize(getReportResponse.json())
        if(payload['status'][0] == 'FATAL'):
            break
        
    if payload['status'][0] == 'COMPLETED':
        
        report_document_id = payload['reportId'][0]
        report_file_url = payload['url'][0]
        # print(report_file_url)
    
        print("Report document ID: ", report_document_id) 
        
        # prepare files names and paths
        output_name =  running_report+'_'+ country + '_' + end_date + '_' + str(running_profile_id) + '_' + company
        output_file = output_name + '.json'
        output_file_csv = output_name + '.csv'
        
        # get the file from Amazon servers
        response = requests.get(report_file_url, stream=True)
        
        # open downloaded file, unpack and save it 
        with open(output_file, "wb") as f:
            with gzip.GzipFile(fileobj=response.raw) as gz:
                shutil.copyfileobj(gz, f)
                
        # open downloaded JSON save it as CSV
        df = pd.read_json(output_file)
        df.to_csv(local_path+output_file_csv, index=False)
        
        # check size and save from local to remote
        # local_filename,todays_date,company,campaign_type,gcs_name
        check_size_and_save(output_file_csv,end_date,company,campaign_type,report_type)
        
        # remove JSON payload from local storage
        os.remove(output_file)

# particular report results
def one_report_results(report, running_report,CLIENT_ID, ACCESS_TOKEN,running_profile_id,start_date,end_date,country,company):
    # Extracting necessary information from the report
    post_data = {key: report[key] for key in report if key != 'reportTypeId'}
    time.sleep(1)
    #print(post_data)

    headers = {
        'Amazon-Advertising-API-ClientId':CLIENT_ID,
        'Amazon-Advertising-API-Scope':str(running_profile_id),
        'Authorization': 'Bearer '+ACCESS_TOKEN,
        'Accept': 'application/vnd.createasyncreportrequest.v3+json',
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json'
    }
    #print(headers)
    
    # configurable elements of report
    data_configuration = {}
    
    keys_to_include = ['adProduct', 'groupBy', 'columns', 'filters','reportTypeId','timeUnit','format']
    # inlcude only elements found
    for key in keys_to_include:
        if key in post_data:
            data_configuration[key] = post_data[key]
    data_configuration['reportTypeId']=running_report
    
    data = {
        "startDate":start_date,
        "endDate":end_date,
        "configuration": data_configuration
    }
    
    #print(data)
    
    # get campaign type: sponsored_brands, sponsored_products, sponsored_display
    campaign_type = post_data['adProduct'].lower()
    
    # Making the POST request
    # this is EU endpoint and needs to be adjusted for North America endpoing
    response = requests.post('https://advertising-api-eu.amazon.com/reporting/reports', headers=headers, json=data)
    # if everything OK proceed with download (if succeds)
    if response.status_code == 200:
        json_data = response.json()
        if 'reportId' in json_data:
            payload = pd.json_normalize(response.json())
            reportId = payload['reportId'][0]
            waitForReportData(reportId, headers,running_profile_id,country,running_report,end_date,company,campaign_type,post_data['localName'])
        else:
            print('Problem with '+running_report)
    
    return None

def find_reports(reports, running_report,CLIENT_ID, ACCESS_TOKEN,running_profile_id,start_date,end_date,country,company):
    """
    Searches through the list of reports for a given reportTypeId.
    If found, it uses the information from that report to make a POST request.
    """

    # Searching for the report with the specified reportTypeId
    for report in reports:
        # run specific report if ALL is not specified
        if running_report !='ALL':
            if report['reportTypeId'] == running_report:
                one_report_results(report, running_report,CLIENT_ID, ACCESS_TOKEN,running_profile_id,start_date,end_date,country,company)
        else: 
            one_report_results(report, report['reportTypeId'],CLIENT_ID, ACCESS_TOKEN,running_profile_id,start_date,end_date,country,company)


    # Return None if no matching report is found
    return None

def main():
    # pickup arguments from command line
    arguments = run_argument_parser()
    
    # set initial variables
    running_company = arguments.company
    running_market  = arguments.market
    running_date    = arguments.specific_date
    running_report  = arguments.specific_report
    
    # get operational variables
    todays_date = date.today().strftime('%Y-%m-%d')
    start_date = get_running_dates(running_date)
    temp_start_date = date.today()-timedelta(days=31)
    temp_end_date = date.today()-timedelta(days=1)
    company_list = get_company(running_company)
    
    # pick up country profiles and credentials
    country_profiles, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN = get_company_profiles(company_list)
    
    # lets get all campaigns for the downloaded date
    get_all_campaigns(country_profiles, start_date, running_company, todays_date, CLIENT_ID, ACCESS_TOKEN)
        
         
    # running though list of profiles or one profile (country name)
    for profiles in range(0,len(country_profiles)):
    # go through profiles and download campaign performance
        working_profile = country_profiles['profileId'][profiles]
        country_code = country_profiles['countryCode'][profiles]
        
        # check if 
        
        if running_market!='ALL':
            print('run specific report for '+ running_market +' market')
            if country_code == running_market:
                find_reports(reports, running_report, CLIENT_ID, ACCESS_TOKEN,working_profile,str(temp_start_date),str(temp_end_date),running_market,running_company)
            else:
                print('Skipping market: '+country_code)
        else:
            print('run all reports') 
            find_reports(reports, 'ALL', CLIENT_ID, ACCESS_TOKEN,working_profile,str(temp_start_date),str(temp_end_date),country_code,running_company)
            
   
# Check if the script is being run directly (not imported)
if __name__ == "__main__":
    # root folder on Google Cloud where to store the files
    gs_path = 'gs://ch_commerce_incoming/global/amazon_media/ads_new'
    # local path
    local_path = './local_advertising/'
    cold_storage_path = 'processed/'
    main()
