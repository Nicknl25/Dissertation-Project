-- MySQL dump 10.13  Distrib 8.0.42, for Win64 (x86_64)
--
-- Host: localhost    Database: financials
-- ------------------------------------------------------
-- Server version	8.0.42

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `aapl_quarterly_revenue`
--

DROP TABLE IF EXISTS `aapl_quarterly_revenue`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `aapl_quarterly_revenue` (
  `fiscalDateEnding` date DEFAULT NULL,
  `totalRevenue` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dissertation_data`
--

DROP TABLE IF EXISTS `dissertation_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dissertation_data` (
  `fiscalDateEnding` date DEFAULT NULL,
  `reportedCurrency.x` varchar(4) DEFAULT NULL,
  `grossProfit` varchar(12) DEFAULT NULL,
  `totalRevenue` varchar(12) DEFAULT NULL,
  `costOfRevenue` varchar(11) DEFAULT NULL,
  `costofGoodsAndServicesSold` varchar(11) DEFAULT NULL,
  `operatingIncome` varchar(11) DEFAULT NULL,
  `sellingGeneralAndAdministrative` varchar(10) DEFAULT NULL,
  `researchAndDevelopment` varchar(11) DEFAULT NULL,
  `operatingExpenses` varchar(11) DEFAULT NULL,
  `investmentIncomeNet` varchar(4) DEFAULT NULL,
  `netInterestIncome` varchar(10) DEFAULT NULL,
  `interestIncome` varchar(10) DEFAULT NULL,
  `interestExpense` varchar(10) DEFAULT NULL,
  `nonInterestIncome` varchar(4) DEFAULT NULL,
  `otherNonOperatingIncome` varchar(11) DEFAULT NULL,
  `depreciation` varchar(4) DEFAULT NULL,
  `depreciationAndAmortization` varchar(11) DEFAULT NULL,
  `incomeBeforeTax` varchar(11) DEFAULT NULL,
  `incomeTaxExpense` varchar(11) DEFAULT NULL,
  `interestAndDebtExpense` varchar(4) DEFAULT NULL,
  `netIncomeFromContinuingOperations` varchar(11) DEFAULT NULL,
  `comprehensiveIncomeNetOfTax` varchar(4) DEFAULT NULL,
  `ebit` varchar(11) DEFAULT NULL,
  `ebitda` varchar(11) DEFAULT NULL,
  `netIncome.x` varchar(11) DEFAULT NULL,
  `symbol` varchar(5) DEFAULT NULL,
  `reportedCurrency.y` varchar(3) DEFAULT NULL,
  `totalAssets` varchar(12) DEFAULT NULL,
  `totalCurrentAssets` varchar(12) DEFAULT NULL,
  `cashAndCashEquivalentsAtCarryingValue` varchar(11) DEFAULT NULL,
  `cashAndShortTermInvestments` varchar(11) DEFAULT NULL,
  `inventory` varchar(11) DEFAULT NULL,
  `currentNetReceivables` varchar(11) DEFAULT NULL,
  `totalNonCurrentAssets` varchar(12) DEFAULT NULL,
  `propertyPlantEquipment` varchar(12) DEFAULT NULL,
  `accumulatedDepreciationAmortizationPPE` varchar(11) DEFAULT NULL,
  `intangibleAssets` varchar(11) DEFAULT NULL,
  `intangibleAssetsExcludingGoodwill` varchar(11) DEFAULT NULL,
  `goodwill` varchar(12) DEFAULT NULL,
  `investments` varchar(4) DEFAULT NULL,
  `longTermInvestments` varchar(12) DEFAULT NULL,
  `shortTermInvestments` varchar(12) DEFAULT NULL,
  `otherCurrentAssets` varchar(12) DEFAULT NULL,
  `otherNonCurrentAssets` varchar(4) DEFAULT NULL,
  `totalLiabilities` varchar(12) DEFAULT NULL,
  `totalCurrentLiabilities` varchar(12) DEFAULT NULL,
  `currentAccountsPayable` varchar(11) DEFAULT NULL,
  `deferredRevenue` varchar(4) DEFAULT NULL,
  `currentDebt` varchar(4) DEFAULT NULL,
  `shortTermDebt` varchar(11) DEFAULT NULL,
  `totalNonCurrentLiabilities` varchar(12) DEFAULT NULL,
  `capitalLeaseObligations` varchar(11) DEFAULT NULL,
  `longTermDebt` varchar(12) DEFAULT NULL,
  `currentLongTermDebt` varchar(12) DEFAULT NULL,
  `longTermDebtNoncurrent` varchar(4) DEFAULT NULL,
  `shortLongTermDebtTotal` varchar(12) DEFAULT NULL,
  `otherCurrentLiabilities` varchar(11) DEFAULT NULL,
  `otherNonCurrentLiabilities` varchar(11) DEFAULT NULL,
  `totalShareholderEquity` varchar(12) DEFAULT NULL,
  `treasuryStock` varchar(11) DEFAULT NULL,
  `retainedEarnings` varchar(12) DEFAULT NULL,
  `commonStock` varchar(12) DEFAULT NULL,
  `commonStockSharesOutstanding` varchar(11) DEFAULT NULL,
  `reportedCurrency` varchar(3) DEFAULT NULL,
  `operatingCashflow` varchar(11) DEFAULT NULL,
  `paymentsForOperatingActivities` varchar(4) DEFAULT NULL,
  `proceedsFromOperatingActivities` varchar(4) DEFAULT NULL,
  `changeInOperatingLiabilities` varchar(4) DEFAULT NULL,
  `changeInOperatingAssets` varchar(4) DEFAULT NULL,
  `depreciationDepletionAndAmortization` varchar(11) DEFAULT NULL,
  `capitalExpenditures` varchar(11) DEFAULT NULL,
  `changeInReceivables` varchar(12) DEFAULT NULL,
  `changeInInventory` varchar(11) DEFAULT NULL,
  `profitLoss` varchar(4) DEFAULT NULL,
  `cashflowFromInvestment` varchar(12) DEFAULT NULL,
  `cashflowFromFinancing` varchar(12) DEFAULT NULL,
  `proceedsFromRepaymentsOfShortTermDebt` varchar(4) DEFAULT NULL,
  `paymentsForRepurchaseOfCommonStock` varchar(4) DEFAULT NULL,
  `paymentsForRepurchaseOfEquity` varchar(4) DEFAULT NULL,
  `paymentsForRepurchaseOfPreferredStock` varchar(4) DEFAULT NULL,
  `dividendPayout` varchar(10) DEFAULT NULL,
  `dividendPayoutCommonStock` varchar(10) DEFAULT NULL,
  `dividendPayoutPreferredStock` varchar(4) DEFAULT NULL,
  `proceedsFromIssuanceOfCommonStock` varchar(4) DEFAULT NULL,
  `proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet` varchar(4) DEFAULT NULL,
  `proceedsFromIssuanceOfPreferredStock` varchar(4) DEFAULT NULL,
  `proceedsFromRepurchaseOfEquity` varchar(12) DEFAULT NULL,
  `proceedsFromSaleOfTreasuryStock` varchar(4) DEFAULT NULL,
  `changeInCashAndCashEquivalents` varchar(12) DEFAULT NULL,
  `changeInExchangeRate` varchar(10) DEFAULT NULL,
  `netIncome.y` varchar(11) DEFAULT NULL,
  `industry` varchar(30) DEFAULT NULL,
  `sharesOutstanding` double DEFAULT NULL,
  `sharePrice` double DEFAULT NULL,
  `pull_date` date DEFAULT NULL,
  UNIQUE KEY `idx_symbol_fiscalDate` (`symbol`,`fiscalDateEnding`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `earnings_calendar`
--

DROP TABLE IF EXISTS `earnings_calendar`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `earnings_calendar` (
  `symbol` varchar(10) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `reportDate` date DEFAULT NULL,
  `fiscalDateEnding` date DEFAULT NULL,
  `estimate` decimal(15,4) DEFAULT NULL,
  `currency` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `earnings_calendar_log`
--

DROP TABLE IF EXISTS `earnings_calendar_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `earnings_calendar_log` (
  `id` int NOT NULL AUTO_INCREMENT,
  `run_timestamp` datetime DEFAULT NULL,
  `records_inserted` int DEFAULT NULL,
  `runtime_seconds` double DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-10-19  9:52:28
