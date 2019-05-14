using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client.Portal.Initialization.Seeders.Tenants.Seeds
{
    public class SolvencyIISeed
    {
        private readonly ClientPortalContext _context;
        private readonly IAzureBlobStorage _storage;
        private readonly string _containerName = ServiceType.SolvencyIIAnalytics.GetValue();
        private readonly LifeLiabilitiesTaskExecutorFactory _lifeLiabilitiesTaskExecutorFactory;
        private readonly IAssetsMarketValueStressTaskInstanceExecutor _assetsMarketValueStressTaskInstanceExecutor;

        private readonly SeedType _seedType;

        //  private readonly bool _performSIIBelCalculations;
        private readonly bool _performSIIAssetsCalculations;

        private const int _workspaceId = 1;
        private int _numberOfLiabilityDataPoints = 10;
        private int _numberOfAssetsDataPoints = 10;

        private List<LifeLiabilitiesInputDataSet> _inputDataSets;
        private Dictionary<LifeLiabilitiesInputDataSet, List<LifeLiabilitiesInputDataSetItemQuery>> _inputDataSetItemsDictionary;

        private MortalityTable _maleMortalityTable;
        private MortalityTable _femaleMortalityTable;

        public SolvencyIISeed(ClientPortalContext context,
            InitializationConfiguration initializationConfiguration,
            IAzureBlobStorage storage,
            IAssetsMarketValueStressTaskInstanceExecutor assetsMarketValueStressTaskInstanceExecutor,
            LifeLiabilitiesTaskExecutorFactory lifeLiabilitiesTaskExecutorFactory)
        {
            _context = context;
            _storage = storage;
            _assetsMarketValueStressTaskInstanceExecutor = assetsMarketValueStressTaskInstanceExecutor;
            _lifeLiabilitiesTaskExecutorFactory = lifeLiabilitiesTaskExecutorFactory;

            _seedType = initializationConfiguration.TenantSeedType;
            _performSIIAssetsCalculations = initializationConfiguration.PerformSIIAssetsCalculations;

            if (_seedType == SeedType.Full)
            {
                _numberOfLiabilityDataPoints = 2;
                _numberOfAssetsDataPoints = 2;
            }

            if (_seedType == SeedType.Limited)
            {
                _numberOfLiabilityDataPoints = 2;
                _numberOfAssetsDataPoints = 2;
            }

            if (_seedType == SeedType.IntegrationTests)
            {
                _numberOfLiabilityDataPoints = 2;
                _numberOfAssetsDataPoints = 2;
            }
        }

        internal async Task SeedAsync()
        {
            var rnd = new Random();

            await CreateBodConfigsAsync();

            await CreateWorkflowConfigsAsync();

            // Begin refactor

            await CreateReportingCyclesAsync();

            await CreateReportingEntitiesAsync();

            await CreateRegulatoryFundsAsync();

            await CreateInvestmentFundsAsync();

            await CreateProductsAsync(rnd);

            await CreateLifeLiabilitiesBodsAsync(rnd);

            await CreateLifeLiabilitiesInputDataSetsAsync(rnd);

            await CreateLifeLiabilitiesScenariosAsync(rnd);

            await CreateMortalityTablesAsync(rnd);

            await CreateSicknessTablesAsync(rnd);

            await CreateYearVectorRatesAsync(rnd);

            await CreateAgeVectorRatesAsync(rnd);

            await CreateYieldCurvesAsync(rnd);

            await CreateLifeLiabilitiesAssumptionsAsync(rnd);

            await CreateLifeLiabilitiesTasksAsync(rnd);

            await DoLiabilityCashflowProjectionTasks(rnd);

            // End refactor

            await CreateAssetsBodConfigsAsync(rnd);

            await CreateAssetsBodInstancesAsync(rnd);

            await CreateAssetsAssumptionInstancesAsync(rnd);

            await CreateAssetsTaskInstancesAsync(rnd); ;

            await DoAssetsStressCalculations(rnd);
        }

        // Begin Refactor
        private async Task CreateReportingCyclesAsync()
        {
            Log.Information("CreateSIIReportingCyclesAsync.Start");

            try
            {
                if (_context.SIIReportingCycles.Any())
                {
                    Log.Information("CreateSIIReportingCyclesAsync.Data");
                    return;
                }

                var dataImportRCConfiguration = await _context.WorkflowConfigs
                        .SingleOrDefaultAsync(r => (r.Reference == "Data Import Workflow") && (r.WorkspaceId == _workspaceId));

                dataImportRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId,
                    reference: "Standard Data Import 2017 Year End",
                    effectiveDate: new DateTime(year: 2017, month: 12, day: 31).Date, status: ReportingCycleStatusType.Finalised, modifier: SystemUsers.Seed));

                await _context.BulkSaveChangesAsync(); // save so that can have Id = RC

                var changeInBasisRC = dataImportRCConfiguration.ReportingCycles
                        .SingleOrDefault(r => r.Reference == "Standard Data Import 2017 Year End");

                dataImportRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId,
                    reference: "Standard Data Import 2018 Year End",
                    effectiveDate: new DateTime(year: 2018, month: 12, day: 31).Date,
                    status: ReportingCycleStatusType.InProgress,
                    changeInBasisReportingCycleReference: changeInBasisRC.Reference,
                    modifier: SystemUsers.Seed));

                if (_seedType == SeedType.IntegrationTests)
                {
                    dataImportRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId, reference: "Test 1", effectiveDate: new DateTime(year: 2017, month: 9, day: 30).Date, status: ReportingCycleStatusType.Finalised, modifier: SystemUsers.Seed));
                    dataImportRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId, reference: "Test 2", effectiveDate: new DateTime(year: 2017, month: 9, day: 30).Date, status: ReportingCycleStatusType.Finalised, modifier: SystemUsers.Seed));
                    dataImportRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId, reference: "Test 3", effectiveDate: new DateTime(year: 2017, month: 9, day: 30).Date, status: ReportingCycleStatusType.Finalised, isDeleted: true, modifier: SystemUsers.Seed));
                }

                var dataEntryRCConfiguration = await _context.WorkflowConfigs
                    .SingleOrDefaultAsync(r => (r.Reference == "Data Entry Workflow") && (r.WorkspaceId == _workspaceId));

                dataEntryRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId, reference: "Data Entry 2018 Q3", effectiveDate: new DateTime(year: 2017, month: 9, day: 30).Date, status: ReportingCycleStatusType.InProgress, modifier: SystemUsers.Seed));

                var dataMappingRCConfiguration = await _context.WorkflowConfigs
                    .SingleOrDefaultAsync(r => (r.Reference == "Data Mapping Workflow") && (r.WorkspaceId == _workspaceId));

                dataMappingRCConfiguration.AddReportingCycle(new ReportingCycle(workspaceId: _workspaceId, reference: "Custom Data Import 2018 Q3", effectiveDate: new DateTime(year: 2017, month: 9, day: 30).Date, status: ReportingCycleStatusType.InProgress, modifier: SystemUsers.Seed));

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateSIIReportingCyclesAsync");
            }

            Log.Information("CreateSIIReportingCyclesAsync.End");
        }

        private async Task CreateReportingEntitiesAsync()
        {
            Log.Information("CreateReportingEntitiesAsync.Start");
            if (_context.SIIReportingEntities.Any())
            {
                Log.Information("CreateReportingEntitiesAsync.Data already exists");
                return;
            }

            try
            {
                var reportingCycles = await _context.SIIReportingCycles
               .ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddReportingEntity(new Domain.Models.SolvencyII.ReportingEntity(workspaceId: _workspaceId, reference: "UK Insurance", modifier: SystemUsers.Seed));
                    reportingCycle.AddReportingEntity(new Domain.Models.SolvencyII.ReportingEntity(workspaceId: _workspaceId, reference: "France Insurance", modifier: SystemUsers.Seed));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateReportingEntitiesAsync");
            }

            Log.Information("CreateReportingEntitiesAsync.End");
        }

        private async Task CreateRegulatoryFundsAsync()
        {
            Log.Information("CreateRegulatoryFundsAsync.Start");
            if (_context.SIIRegulatoryFunds.Any())
            {
                Log.Information("CreateRegulatoryFundsAsync.Data already exists");
                return;
            }

            try
            {
                var reportingCycles = await _context.SIIReportingCycles
                    .Where(r => r.WorkspaceId == _workspaceId)
                    .ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    var ukRE = await _context.SIIReportingEntities.SingleOrDefaultAsync(r => r.Reference == "UK Insurance" && r.ReportingCycleId == reportingCycle.Id);

                    ukRE.AddRegulatoryFund(new Domain.Models.SolvencyII.RegulatoryFund(workspaceId: _workspaceId,
                        reference: "Heritage WP", type: RegulatoryFundType.WithProfit, modifier: SystemUsers.Seed));

                    ukRE.AddRegulatoryFund(new Domain.Models.SolvencyII.RegulatoryFund(workspaceId: _workspaceId,
                        reference: "Non-Profit Business", type: RegulatoryFundType.NonProfit, modifier: SystemUsers.Seed));

                    ukRE.AddRegulatoryFund(new Domain.Models.SolvencyII.RegulatoryFund(workspaceId: _workspaceId,
                        reference: "Investment Business", type: RegulatoryFundType.NonProfit, modifier: SystemUsers.Seed));

                    var franceRE = await _context.SIIReportingEntities.SingleOrDefaultAsync(r => r.Reference == "France Insurance" && r.ReportingCycleId == reportingCycle.Id);

                    franceRE.AddRegulatoryFund(new Domain.Models.SolvencyII.RegulatoryFund(workspaceId: _workspaceId,
                        reference: "Unit Linked", type: RegulatoryFundType.NonProfit, modifier: SystemUsers.Seed));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateRegulatoryFundsAsync");
            }

            Log.Information("CreateRegulatoryFundsAsync.End");
        }

        private async Task CreateInvestmentFundsAsync()
        {
            Log.Information("CreateInvestmentFundsAsync.Start");

            try
            {
                if (_context.SIIInvestmentFunds.Any())
                {
                    Log.Information("CreateInvestmentFundsAsync.Data already exists");
                    return;
                }

                var regulatoryFunds = await _context.SIIRegulatoryFunds.ToListAsync();

                foreach (var regulatoryFund in regulatoryFunds)
                {
                    if (_seedType == SeedType.IntegrationTests)
                    {
                        regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "Test 1", annualManagementChargePercentage: 0m, modifier: SystemUsers.Seed));
                        regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "Test 2", annualManagementChargePercentage: 0m, modifier: SystemUsers.Seed));
                    }

                    switch (regulatoryFund.Reference)
                    {
                        case "Investment Business":
                            regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "UK Mixed Equity", annualManagementChargePercentage: 0m, modifier: SystemUsers.Seed));
                            regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "Euro Mixed Equity", annualManagementChargePercentage: 0m, modifier: SystemUsers.Seed));
                            regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "UK Property", annualManagementChargePercentage: 0m, modifier: SystemUsers.Seed));
                            regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "UK Fixed Income", annualManagementChargePercentage: 0m, modifier: SystemUsers.Seed));
                            break;

                        case "Unit Linked":
                            regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "Euro Development Fund", annualManagementChargePercentage: 0.005m, bidOfferSpreadPercentage: 0.05m, modifier: SystemUsers.Seed));
                            regulatoryFund.AddInvestmentFund(new Domain.Models.SolvencyII.InvestmentFund(workspaceId: _workspaceId, reference: "France High Tech", annualManagementChargePercentage: 0.01m, modifier: SystemUsers.Seed));
                            break;
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Information($"CreateInvestmentFundsAsync error: {e.Message}");
            }

            Log.Information("CreateInvestmentFundsAsync.End");
        }

        private Domain.Models.SolvencyII.ProductInvestmentFund CreateProductInvestmentFund(List<Domain.Models.SolvencyII.InvestmentFund> investmentFunds, string investmentFundReference)
        {
            var investmentFund = investmentFunds.SingleOrDefault(i => (i.Reference == investmentFundReference));

            if ((investmentFund != null))
            {
                return new Domain.Models.SolvencyII.ProductInvestmentFund(workspaceId: _workspaceId,
                     reference: investmentFund.Reference, investmentFundId: investmentFund.Id, modifier: SystemUsers.Seed);
            }
            else
            {
                Log.Error($"CreateProductInvestmentFundsAsync error - no match for InvestmentFund {investmentFundReference}");
                return null;
            }
        }

        private async Task CreateProductsAsync(Random rnd)
        {
            Log.Information("CreateProductsAsync.Start");

            try
            {
                if (_context.SIIProducts.Any())
                {
                    Log.Information("CreateProductsAsync.Data already exists");
                    return;
                }

                var regulatoryFunds = await _context.SIIRegulatoryFunds.ToListAsync();

                foreach (var regulatoryFund in regulatoryFunds)
                {
                    var products = new List<Domain.Models.SolvencyII.Product>();
                    var investmentFunds = await _context.SIIInvestmentFunds.Where(i => (i.RegulatoryFundId == regulatoryFund.Id)).ToListAsync();

                    switch (regulatoryFund.Reference)
                    {
                        case "Heritage WP":
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "CWP - Endowment", group: "CWP", type: ProductType.CWP,
                                commissionPercentage: 2m, cwpRegularBonus: CWPRegularBonusType.Compound,
                                guaranteedRegularBonusRatePercentage: 0.06m,
                                modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "CWP - Whole of Life", group: "CWP", type: ProductType.CWP,
                                commissionPercentage: 2m, cwpRegularBonus: CWPRegularBonusType.Simple, hasMaturityBenefit: false,
                                modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Unitised With Profits", group: "UWP", type: ProductType.UWP,
                                hasMaturityBenefit: true,
                                commissionPercentage: 1.5m, annualManagementChargePercentage: 0.01m,
                                minDeathBenefitMultSumAssuredPercentage: 1.01m, minDeathBenefitMultBonusesPercentage: 1m,
                                mvaFreeDateFirst: 10, mvaFreeDateStep: 5,
                                partialWithdrawalPenaltyFreePercentage: 0.05m,
                                modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "LISA - With Profits", group: "With Profits", type: ProductType.CWP, modifier: SystemUsers.Seed));
                            break;

                        case "Non-Profit Business":
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "LTA", group: "Term Assurance", type: ProductType.TermAssurance,
                                hasMaturityBenefit: false, modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "PHI", group: "Sickness",
                                type: ProductType.PHI,
                                sicknessBenefitsCeaseAge: 65,
                                first26WeeksSicknessBenefitPercentage: 1m,
                                weeks27To52SicknessBenefitPercentage: 0.5m,
                                after52SicknessBenefitPercentage: 0.25m,
                                fixedSurrenderPenalty: 10,
                                modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Holloway Sickness Insurance Type 1", group: "Holloway Sickness",
                                type: ProductType.HollowaySicknessType1,
                                hasMaturityBenefit: false,
                                sicknessBenefitsCeaseAge: 65,
                                first26WeeksSicknessBenefitPercentage: 1m,
                                weeks27To52SicknessBenefitPercentage: 0.5m,
                                after52SicknessBenefitPercentage: 0.25m,
                                fixedSurrenderPenalty: 10,
                                modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Holloway Sickness Insurance Type 2", group: "Holloway Sickness",
                               type: ProductType.HollowaySicknessType2,
                               hasMaturityBenefit: false,
                               sicknessBenefitsCeaseAge: 70,
                               modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Sickness Interest Only", group: "Holloway Interest",
                                type: ProductType.HollowaySicknessType1,
                                hasMaturityBenefit: false,
                                sicknessBenefitsCeaseAge: 65,
                                fixedSurrenderPenalty: 10,
                                modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Deferred Annuity", group: "Annuity", type: ProductType.Annuity, hasMaturityBenefit: false, modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Enhanced Annuity", group: "Annuity", type: ProductType.Annuity, hasMaturityBenefit: false, modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Non Profits - Whole of Life", group: "Conventional", type: ProductType.CNP, hasMaturityBenefit: false, modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Non Profits - Endowment", group: "Conventional", type: ProductType.CNP, modifier: SystemUsers.Seed));
                            products.Add(new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "Guaranteed Investment Bond", group: "GIB", type: ProductType.NonProfitBond,
                                 hasMaturityBenefit: true, guaranteedInvestmentReturnPercentage: 0.04m,
                                modifier: SystemUsers.Seed));

                            break;

                        case "Investment Business":

                            var productLISA1 = new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "LISA - High Yield", group: "LISA", type: ProductType.UL,
                                ulAllocationRate: 0.9m, deathBenefitMultFundValuePercentage: 1.01m, doesPolicyCeaseOnFundGoingNegative: true, modifier: SystemUsers.Seed);
                            productLISA1.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "UK Mixed Equity"));
                            productLISA1.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "Euro Mixed Equity"));
                            products.Add(productLISA1);

                            if (_seedType != SeedType.IntegrationTests)
                            {
                                var productLISA2 = new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "LISA - Low Yield", group: "LISA", type: ProductType.UL, modifier: SystemUsers.Seed);
                                productLISA2.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "UK Property"));
                                productLISA2.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "UK Fixed Income"));
                                products.Add(productLISA2);
                            }

                            var productCTF1 = new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "CTF - Mixed", group: "CTF", type: ProductType.UL,
                                policyFee: 60m, ulAllocationRate: 0.95m, modifier: SystemUsers.Seed);
                            productCTF1.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "UK Property"));
                            productCTF1.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "Euro Mixed Equity"));
                            products.Add(productCTF1);

                            if (_seedType != SeedType.IntegrationTests)
                            {
                                var productCTF2 = new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "CTF - EUR Mixed", group: "CTF", type: ProductType.UL, modifier: SystemUsers.Seed);
                                productCTF2.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "Euro Mixed Equity"));
                                products.Add(productCTF2);
                                break;
                            }

                            break;

                        case "Unit Linked":
                            if (_seedType != SeedType.IntegrationTests)
                            { // don't create when do UTs as don't create input data items etc
                                var productUL1 = new Domain.Models.SolvencyII.Product(workspaceId: _workspaceId, reference: "UL Endowment", group: "Unit Linked", type: ProductType.UL,
                                   ulAllocationRate: 0.95m, policyFee: 24m,
                                   modifier: SystemUsers.Seed);
                                productUL1.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "Euro Development Fund"));
                                productUL1.AddInvestmentFund(CreateProductInvestmentFund(investmentFunds: investmentFunds, investmentFundReference: "France High Tech"));
                                products.Add(productUL1);
                            }
                            break;
                    }

                    foreach (var product in products)
                    {
                        int numCommissionClawback = _seedType == SeedType.IntegrationTests
                            ? 2
                            : (product.Reference == "Unitised With Profits") ? rnd.Next(2, 4) : rnd.Next(0, 4);

                        for (int year = 1; year <= numCommissionClawback; year++)
                        {
                            product.AddYearRate(new Domain.Models.SolvencyII.ProductYearRate(workspaceId: _workspaceId, type: ProductYearRateType.CommissionClawback, year: year, rate: Math.Max(1m - (0.25m) * (year - 1), 0m), modifier: "seed"));
                        }

                        if ((product.Reference == "Unitised With Profits") || (product.Reference == "LISA - High Yield"))
                        {
                            for (int year = 1; year <= 4; year++)
                            {
                                product.AddYearRate(new Domain.Models.SolvencyII.ProductYearRate(workspaceId: _workspaceId, type: ProductYearRateType.SurrenderPenalty, year: year, rate: Math.Max(0.05m - (0.01m) * (year - 1), 0m), modifier: "seed"));
                            }
                        }

                        if (product.Reference == "Guaranteed Investment Bond")
                        {
                            for (int year = 1; year <= 5; year++)
                            {
                                product.AddYearRate(new Domain.Models.SolvencyII.ProductYearRate(workspaceId: _workspaceId, type: ProductYearRateType.DeathBenefitMultContributionPercentage, year: year, rate: Math.Max(1m + 0.01m * (year - 1), 1m), modifier: "seed"));
                                product.AddYearRate(new Domain.Models.SolvencyII.ProductYearRate(workspaceId: _workspaceId, type: ProductYearRateType.SurrenderValueContributionReturnPercentage, year: year, rate: Math.Min(0.005m * year, 1m), modifier: "seed"));
                            }
                        }

                        regulatoryFund.AddProduct(product);
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Information($"CreateProductsAsync error: {e.Message}");
            }

            Log.Information("CreateProductsAsync.End");
        }

        private async Task CreateLifeLiabilitiesBodsAsync(Random rnd)
        {
            Log.Information("CreateLifeLiabilitiesBodsAsync.Start");

            try
            {
                if (_context.LifeLiabilitiesBods.Any())
                {
                    Log.Information("CreateLifeLiabilitiesBodsAsync.Data already exists");
                    return;
                }

                var regulatoryFunds = await _context.SIIRegulatoryFunds.ToListAsync();

                foreach (var regulatoryFund in regulatoryFunds)
                {
                    switch (regulatoryFund.Reference)
                    {
                        case "Heritage WP":
                            regulatoryFund.AddLifeLiabilitiesBod(new LifeLiabilitiesBod(workspaceId: _workspaceId, reference: "Conventional With Profit", hasDiscretionaryBonuses: true,
                                cwpSurrenderValueMethod: CWPSurrenderValueMethodType.AssetShare, modifier: SystemUsers.Seed));
                            regulatoryFund.AddLifeLiabilitiesBod(new LifeLiabilitiesBod(workspaceId: _workspaceId, reference: "Unitised With Profit", hasDiscretionaryBonuses: true, modifier: SystemUsers.Seed));
                            break;

                        case "Non-Profit Business":
                            regulatoryFund.AddLifeLiabilitiesBod(new LifeLiabilitiesBod(workspaceId: _workspaceId, reference: "Non Profit", modifier: SystemUsers.Seed));
                            regulatoryFund.AddLifeLiabilitiesBod(new LifeLiabilitiesBod(workspaceId: _workspaceId, reference: "Sickness", hasMorbidityRisk: true, modifier: SystemUsers.Seed));
                            break;

                        case "Investment Business":
                            regulatoryFund.AddLifeLiabilitiesBod(new LifeLiabilitiesBod(workspaceId: _workspaceId, reference: "Investment Business", hasNonInterestMarketRisk: true, modifier: SystemUsers.Seed));
                            break;

                        case "Unit Linked":
                            regulatoryFund.AddLifeLiabilitiesBod(new LifeLiabilitiesBod(workspaceId: _workspaceId, reference: "Unit Linked", hasNonInterestMarketRisk: true, modifier: SystemUsers.Seed));
                            break;
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Information($"CreateLifeLiabilitiesBodsAsync error: {e.Message}");
            }

            Log.Information("CreateLifeLiabilitiesBodsAsync.End");
        }

        private async Task CreateLifeLiabilitiesBodImportFile(LifeLiabilitiesBod bod)
        {
            string header = "Policy ID,Note,Product Code,Date Of Entry,Date Of Maturity,Gender Life 1,Date Of Birth Life 1,Gender Life 2,Date Of Birth Life 2,Contribution,Contribution Frequency,Contribution Cease Date,Sum Assured,Regular Bonus,Asset Share,Minimum Death Benefit,";
            header += "Annuity,Annuity Payment Frequency,Is Joint Life Annuity,Spouse Annuity Percentage,Annuity Vesting Date,Annuity Increase In Deferment Percentage,Annuity Increase In Payment Percentage,";
            for (int i = 1; i <= 10; i++)
            {
                header += $"Investment Fund {i},Number Of Units {i},Contribution Allocation Percentage {i},";
            }

            header += "Sickness Deferment Period,Sickness Benefit,Sickness Benefit Covered By Medical Account,Spouse Death Benefit,Medical Account Number Of Shares,Medical Account Shares Accrue Each Month,Medical Account Max Number Of Shares";
            header += "Medical Account Value,Interest Account Contribution,Interest Account Value,Number of Policies,Include";

            var csv = new StringBuilder(header);

            foreach (var kvp in _inputDataSetItemsDictionary)
            {
                var products = await _context.SIIProducts
                    .ToListAsync();

                var inputDataSetItems = kvp.Value;

                foreach (var item in inputDataSetItems)
                {
                    var product = products.Single(p => p.Id == item.ProductId);

                    var rowContents = new StringBuilder();
                    rowContents.Append(item.Reference); //Policy Number
                    rowContents.Append($",{item.Note}"); //Note
                    rowContents.Append($",{product.Reference}"); //Product
                    rowContents.Append($",{item.DateOfEntry:dd/MM/yyyy}");// ,Date of Entry
                    rowContents.Append(item.DateOfMaturity.HasValue ? $",{item.DateOfMaturity.Value.ToString("dd/MM/yyyy")}" : ",");// ,DateOfMaturity

                    rowContents.Append($",{item.GenderLife1.GetDescription()}"); //,Gender 1
                    rowContents.Append($",{item.DateOfBirthLife1:dd/MM/yyyy}");// ,Date Of Birth 1

                    rowContents.Append(item.GenderLife2.HasValue ? $",{item.GenderLife2.Value.GetDescription()}" : ","); //,Gender 2
                    rowContents.Append(item.DateOfBirthLife2.HasValue ? $",{item.DateOfBirthLife2.Value.ToString("dd/MM/yyyy")}" : ",");// ,Date Of Birth 2

                    rowContents.Append(item.Contribution.HasValue ? $",{item.Contribution.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,Contribution
                    rowContents.Append(item.ContributionFrequency.HasValue ? $",{item.ContributionFrequency.Value.GetDescription()}" : ",");// Contribution Frequency
                    rowContents.Append(item.ContributionCeaseDate.HasValue ? $",{item.ContributionCeaseDate.Value.ToString("dd/MM/yyyy")}" : ",");// Contribution cease date
                    rowContents.Append(item.SumAssured.HasValue ? $",{item.SumAssured.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,SumAssured
                    rowContents.Append(item.RegularBonus.HasValue ? $",{item.RegularBonus.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,RegularBonus
                    rowContents.Append(item.AssetShare.HasValue ? $",{item.AssetShare.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,AssetShare
                    rowContents.Append(item.MinDeathBenefit.HasValue ? $",{item.MinDeathBenefit.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,MinDeathBenefit

                    rowContents.Append(item.Annuity.HasValue ? $",{item.Annuity.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,Annuity
                    rowContents.Append(item.AnnuityPaymentFrequency.HasValue ? $",{item.AnnuityPaymentFrequency.Value.GetDescription()}" : ",");// AnnuityPaymentFrequency
                    rowContents.Append(item.IsJointLifeAnnuity.HasValue ? $",{item.IsJointLifeAnnuity}" : ",");// IsJointLifeAnnuity
                    rowContents.Append(item.SpouseAnnuityPercentage.HasValue ? $",{item.SpouseAnnuityPercentage.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,SpouseAnnuityPercentage
                    rowContents.Append(item.AnnuityVestingDate.HasValue ? $",{item.AnnuityVestingDate.Value.ToString("dd/MM/yyyy")}" : ",");// ,Date Of Birth 2
                    rowContents.Append(item.AnnuityIncreaseInDefermentPercentage.HasValue ? $",{item.AnnuityIncreaseInDefermentPercentage.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,AnnuityIncreaseInDefermentPercentage
                    rowContents.Append(item.AnnuityIncreaseInPaymentPercentage.HasValue ? $",{item.AnnuityIncreaseInPaymentPercentage.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,AnnuityIncreaseInPaymentPercentage

                    foreach (var investmentFundAllocation in item.InvestmentFundAllocations)
                    {
                        var investmentFund = await _context.SIIInvestmentFunds
                            .SingleOrDefaultAsync(i => i.Id == investmentFundAllocation.InvestmentFundId);

                        rowContents.Append($",{investmentFund.Reference}"); //Investment fund
                        rowContents.Append(investmentFundAllocation.NumberOfUnits.HasValue
                            ? $",{investmentFundAllocation.NumberOfUnits.Value.ToString("F5", CultureInfo.InvariantCulture)}"
                            : "");//,NumberOfUnits
                        rowContents.Append(investmentFundAllocation.ContributionAllocationPercentage.HasValue
                            ? $",{investmentFundAllocation.ContributionAllocationPercentage.Value.ToString("F2", CultureInfo.InvariantCulture)}"
                            : "");//,ContributionAllocationPercentage
                    }

                    for (int i = item.InvestmentFundAllocations.Count + 1; i <= 10; i++)
                    {
                        rowContents.Append($",,,");
                    }

                    rowContents.Append(item.SicknessDefermentPeriod.HasValue ? $",{item.SicknessDefermentPeriod.Value.GetDescription()}" : ",");//,Sickness deferment period
                    rowContents.Append(item.SicknessBenefit.HasValue ? $",{item.SicknessBenefit.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,Sickness benefit
                    rowContents.Append(item.SicknessBenefitCoveredByMedicalAccount.HasValue ? $",{item.SicknessBenefitCoveredByMedicalAccount.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,SicknessBenefitCoveredByMedicalAccount
                    rowContents.Append(item.SpouseDeathBenefit.HasValue ? $",{item.SpouseDeathBenefit.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,Spouse Death benefit
                    rowContents.Append(item.MedicalAccountNumberShares.HasValue ? $",{item.MedicalAccountNumberShares.Value.ToString("F0", CultureInfo.InvariantCulture)}" : ",");//,MedicalAccountNumberShares
                    rowContents.Append(item.MedicalAccountSharesAcccrueEachMonth.HasValue ? $",{item.MedicalAccountSharesAcccrueEachMonth.Value.ToString("F0", CultureInfo.InvariantCulture)}" : ",");//,MedicalAccountSharesAcccrueEachMonth
                    rowContents.Append(item.MedicalAccountMaxNumberShares.HasValue ? $",{item.MedicalAccountMaxNumberShares.Value.ToString("F0", CultureInfo.InvariantCulture)}" : ",");//,MedicalAccountMaxNumberShares
                    rowContents.Append(item.MedicalAccountValue.HasValue ? $",{item.MedicalAccountValue.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,MedicalAccountMaxNumberShares
                    rowContents.Append(item.InterestAccountContribution.HasValue ? $",{item.InterestAccountContribution.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,MedicalAccountMaxNumberShares
                    rowContents.Append(item.MedicalAccountValue.HasValue ? $",{item.MedicalAccountValue.Value.ToString("F2", CultureInfo.InvariantCulture)}" : ",");//,MedicalAccountMaxNumberShares

                    rowContents.Append($",{item.NumberOfPolicies.ToString("F2", CultureInfo.InvariantCulture)}");
                    rowContents.Append($",{item.IncludeInProjection}");

                    csv.Append($"\n{rowContents}");
                }
            }
            await _storage.UploadAsync(containerName: ServiceType.SolvencyIIAnalytics.GetValue(),
                fileStorageId: bod.UniqueId,
                text: csv.ToString());
        }

        private async Task<LifeLiabilitiesInputDataSet> GetInputDataSetAsync(LifeLiabilitiesBod bod,
            Domain.Models.SolvencyII.Product product)
        {
            var inputDataSet = _inputDataSets.SingleOrDefault(x => x.ProductId == product.Id);

            if (inputDataSet == null)
            {
                inputDataSet = new LifeLiabilitiesInputDataSet(workspaceId: _workspaceId,
                     productId: product.Id);

                bod.AddInputDataSet(inputDataSet);
                _inputDataSets.Add(inputDataSet);
                await _context.BulkSaveChangesAsync(); // to get inputDataSet.Id
            }

            return inputDataSet;
        }

        private void AddToInputDataSet(LifeLiabilitiesInputDataSet inputDataSet, LifeLiabilitiesInputDataSetItemQuery inputDataSetItem)
        {
            List<LifeLiabilitiesInputDataSetItemQuery> inputDataSetItems;

            if (_inputDataSetItemsDictionary.ContainsKey(inputDataSet))
            {
                inputDataSetItems = _inputDataSetItemsDictionary[inputDataSet];
            }
            else
            {
                inputDataSetItems = new List<LifeLiabilitiesInputDataSetItemQuery>();
                _inputDataSetItemsDictionary.Add(inputDataSet, inputDataSetItems);
            }

            inputDataSetItems.Add(inputDataSetItem);
        }

        private async Task StoreInputDataSetItems()
        {
            foreach (var kvp in _inputDataSetItemsDictionary)
            {
                var json = JsonConvert.SerializeObject(kvp.Value);
                await _storage.UploadAsync(containerName: _containerName, fileStorageId: kvp.Key.UniqueId, text: json);
                kvp.Key.SetNumberDatatems(kvp.Value.Count());
            }
        }

        private async Task CreateLifeLiabilitiesInputDataSetsAsync(Random rnd)
        {
            Log.Information("CreateLifeLiabilitiesInputDataSet.Start");

            try
            {
                var bods = await _context.LifeLiabilitiesBods
                    .Include(b => b.RegulatoryFund)
                        .ThenInclude(b => b.ReportingEntity)
                            .ThenInclude(b => b.ReportingCycle)
                    .ToListAsync();

                foreach (var bod in bods)
                {
                    var products = await _context.SIIProducts
                        .Include(p => p.InvestmentFunds)
                        .Where(p => p.RegulatoryFundId == bod.RegulatoryFundId)
                        .ToListAsync();

                    _inputDataSets = new List<LifeLiabilitiesInputDataSet>();
                    _inputDataSetItemsDictionary = new Dictionary<LifeLiabilitiesInputDataSet, List<LifeLiabilitiesInputDataSetItemQuery>>();

                    bod.AddInputDataSets(_inputDataSets);

                    int orderNumber = 0;
                    if (bod.Reference == "Non Profit")
                    {
                        var productReference = "LTA";

                        var product = products
                                .SingleOrDefault(p => p.Reference == productReference);

                        if (product == null)
                        {
                            continue;
                        }

                        var inputDataSet = await GetInputDataSetAsync(bod, product);

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"TA{rnd.Next(10000, 999999)}",
                                    note: "From XYZ admin system",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfMaturity: null,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: null,
                                    genderLife2: null,
                                    contribution: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 500, 2),
                                    contributionFrequency: rnd.NextDouble() < 0.5 ? ContributionFrequencyType.Annual : ContributionFrequencyType.Monthly,
                                    sumAssured: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 300000, 0),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            productReference = rnd.NextDouble() < 0.5 ? "Non Profits - Whole of Life" : "Non Profits - Endowment";

                            // force 1 of each products - nb for cashflow set tests
                            productReference = i == 1 ? "Non Profits - Whole of Life" : i == 2 ? "Non Profits - Endowment" : productReference;

                            product = products
                                .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }
                            inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            bool has2ndLife = (i == 2 || (rnd.NextDouble() < 0.25 && i != 1));

                            var dob2 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife2 = genderLife1 == GenderType.Male ? GenderType.Female : GenderType.Male;
                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"NP{rnd.Next(10000, 999999)}",
                                    note: "From XYZ admin system",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfMaturity: productReference == "Non Profits - Endowment" ? (DateTime?)doe.AddYears(25) : null,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: has2ndLife ? (DateTime?)dob2 : null,
                                    genderLife2: has2ndLife ? (GenderType?)genderLife2 : null,
                                    contribution: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 500, 2),
                                    contributionFrequency: rnd.NextDouble() < 0.5 ? ContributionFrequencyType.Annual : ContributionFrequencyType.Monthly,
                                    contributionCeaseDate: rnd.NextDouble() < 0.5 ? null : (DateTime?)doe.AddYears(rnd.Next(40, 60)),
                                    sumAssured: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 300000, 0),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            productReference = "Guaranteed Investment Bond";

                            product = products
                                   .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2016, 2018), month: rnd.Next(1, 12), day: 1); ;
                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"GIB{rnd.Next(10000, 999999)}",
                                    note: "From XYZ admin system",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfMaturity: doe.AddYears(5),
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    contribution: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 500, 2),
                                    contributionFrequency: ContributionFrequencyType.Single,
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            productReference = rnd.NextDouble() < 0.5 ? "Deferred Annuity" : "Enhanced Annuity";

                            // force 1 of each products - nb for cashflow set tests
                            productReference = i == 1 ? "Deferred Annuity" : i == 2 ? "Enhanced Annuity" : productReference;

                            product = products
                                    .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            bool has2ndLife = (i == 2 || (rnd.NextDouble() < 0.25 && i != 1));

                            var dob2 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife2 = genderLife1 == GenderType.Male ? GenderType.Female : GenderType.Male;
                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"ANN{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: has2ndLife ? (DateTime?)dob2 : null,
                                    genderLife2: has2ndLife ? (GenderType?)genderLife2 : null,
                                    annuity: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 10000, 0),
                                    annuityPaymentFrequency: rnd.NextDouble() < 0.2 ? AnnuityPaymentFrequencyType.Annual : AnnuityPaymentFrequencyType.Monthly,
                                    isJointLifeAnnuity: has2ndLife,
                                    spouseAnnuityPercentage: has2ndLife ? (decimal?)0.5m : null,
                                    annuityVestingDate: (DateTime?)(new DateTime(year: rnd.Next(2000, 2022), month: rnd.Next(1, 12), day: 1)),
                                    annuityIncreaseInDefermentPercentage: (decimal?)(rnd.Next(1, 5) / 100.0),
                                    annuityIncreaseInPaymentPercentage: (decimal?)(rnd.Next(1, 5) / 100.0),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };
                    }
                    else if (bod.Reference == "Unitised With Profit")
                    {
                        orderNumber = 0;
                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = "Unitised With Profits";

                            var product = products
                                   .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2015, 2017), month: rnd.Next(1, 12), day: 1);

                            decimal contribution = (decimal)rnd.Next(1, 3) * 10000;

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"UWP{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfMaturity: (DateTime?)doe.AddYears(20).AddDays(-1),
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: null,
                                    genderLife2: null,
                                    contribution: contribution,
                                    contributionFrequency: ContributionFrequencyType.Single,
                                    contributionCeaseDate: null,
                                    sumAssured: Math.Round(1.01m * contribution, 2),
                                    minDeathBenefit: rnd.Next(2000),
                                    regularBonus: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 100000, 0),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };
                    }
                    else if (bod.Reference == "Conventional With Profit")
                    {
                        orderNumber = 0;
                        List<string> productReferences = new List<string>()
                        {
                            "CWP - Endowment",
                            "CWP - Whole of Life"
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = i == 1
                                ? productReferences[0]
                                : i == 2
                                    ? productReferences[1]
                                    : productReferences[rnd.Next(0, 1)];

                            var product = products
                                    .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2015, 2017), month: rnd.Next(1, 12), day: 1);

                            decimal contribution = (decimal)rnd.Next(1, 3) * 1000;

                            ContributionFrequencyType contributionFrequency = i == 1
                                ? ContributionFrequencyType.Monthly
                                : i == 2
                                    ? ContributionFrequencyType.Annual
                                    : rnd.NextDouble() < 0.5 ? ContributionFrequencyType.Monthly : ContributionFrequencyType.Annual;

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"CWP{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfMaturity: i == 1 ? (DateTime?)doe.AddYears(20).AddDays(-1) : i == 2 ? null : rnd.NextDouble() < 0.5 ? (DateTime?)doe.AddYears(20).AddDays(-1) : null,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: null,
                                    genderLife2: null,
                                    contribution: contribution,
                                    contributionFrequency: contributionFrequency,
                                    contributionCeaseDate: null,
                                    sumAssured: contribution * 1000,
                                    assetShare: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 100000, 0),
                                    regularBonus: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 100000, 0),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };
                    }
                    else if (bod.Reference == "Investment Business")
                    {
                        orderNumber = 0;

                        List<string> productReferences = new List<string>()
                        {
                            "CTF - Mixed",
                            "LISA - High Yield"
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = i == 1
                                ? productReferences[0]
                                : i == 2
                                    ? productReferences[1]
                                    : productReferences[rnd.Next(0, 1)];

                            var product = products
                                   .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            bool has2ndLife = (i == 2 || (rnd.NextDouble() < 0.25 && i != 1));

                            var dob2 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife2 = genderLife1 == GenderType.Male ? GenderType.Female : GenderType.Male;

                            var doe = new DateTime(year: rnd.Next(2015, 2017), month: rnd.Next(1, 12), day: 1);
                            decimal contribution = (decimal)rnd.Next(1, 3) * 1000;
                            decimal? sumAssured = contribution * 1000;

                            ContributionFrequencyType contributionFrequency = i == 1
                                ? ContributionFrequencyType.Monthly
                                : i == 2
                                    ? ContributionFrequencyType.Annual
                                    : rnd.NextDouble() < 0.5 ? ContributionFrequencyType.Monthly : ContributionFrequencyType.Annual;

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                  inputDataSetId: inputDataSet.Id,
                                    id: i,
                                  reference: $"UL{rnd.Next(10000, 999999)}",
                                  productId: product.Id,
                                  productType: product.Type,
                                  productReference: product.Reference,
                                  dateOfEntry: doe,
                                  dateOfMaturity: (DateTime?)doe.AddYears(rnd.Next(1, 2) * 10).AddDays(-1),
                                  dateOfBirthLife1: dob1,
                                  genderLife1: genderLife1,
                                  dateOfBirthLife2: has2ndLife ? (DateTime?)dob2 : null,
                                  genderLife2: has2ndLife ? (GenderType?)genderLife2 : null,
                                  contribution: contribution,
                                  contributionFrequency: contributionFrequency,
                                  contributionCeaseDate: null,
                                  sumAssured: i == 1 ? sumAssured : i == 2 ? null : rnd.Next() < 0.5 ? sumAssured : null,
                                  numberOfPolicies: rnd.Next(10, 100),
                                  orderNumber: orderNumber);

                            decimal? totalAllocated = 0;

                            for (int f = 0; f < product.InvestmentFunds.Count; f++)
                            {
                                var investmentFundAllocation = new LifeLiabilitiesInputDataSetItemInvestmentFundAllocationQuery(workspaceId: _workspaceId,
                                        inputDataSetItemId: i,
                                        id: f + 1,
                                        reference: product.InvestmentFunds[f].InvestmentFund.Reference,
                                        investmentFundId: product.InvestmentFunds[f].InvestmentFundId,
                                        investmentFundReference: product.InvestmentFunds[f].InvestmentFund.Reference,
                                        numberOfUnits: (decimal)Math.Round(rnd.NextDouble() * 1000, 5),
                                        contributionAllocationPercentage: Math.Round((f + 1 == product.InvestmentFunds.Count) ? 1 - (totalAllocated ?? 0) : 1 / (decimal)product.InvestmentFunds.Count, 2));

                                totalAllocated += investmentFundAllocation.ContributionAllocationPercentage;

                                dataItem.InvestmentFundAllocations.Add(investmentFundAllocation);
                            }

                            AddToInputDataSet(inputDataSet, dataItem);
                        };
                    }
                    else if (bod.Reference == "Sickness")
                    {
                        orderNumber = 0;
                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = "PHI";

                            var product = products
                                    .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            decimal contribution = (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 100, 2);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"PHI{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfMaturity: dob1.AddYears(70),
                                    dateOfBirthLife2: null,
                                    genderLife2: null,
                                    contribution: contribution,
                                    contributionFrequency: ContributionFrequencyType.Monthly,
                                    sumAssured: null,
                                    sicknessDefermentPeriod: SicknessDefermentPeriodType.FourWeeks,
                                    sicknessBenefit: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 100, 0),
                                    spouseDeathBenefit: null,
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = "Holloway Sickness Insurance Type 1";

                            var product = products
                                     .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            bool has2ndLife = (i == 2 || (rnd.NextDouble() < 0.25 && i != 1));

                            var dob2 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife2 = genderLife1 == GenderType.Male ? GenderType.Female : GenderType.Male;
                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            decimal contribution = (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 100, 2);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"HS{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: has2ndLife ? (DateTime?)dob2 : null,
                                    genderLife2: has2ndLife ? (GenderType?)genderLife2 : null,
                                    contribution: contribution,
                                    contributionFrequency: ContributionFrequencyType.Monthly,
                                    sumAssured: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 10000, 0),
                                    sicknessBenefit: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 100, 0),
                                    spouseDeathBenefit: has2ndLife ? (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 10000, 0) : null,
                                    medicalAccountNumberShares: (int?)rnd.Next(100, 1000),
                                    medicalAccountSharesAcccrueEachMonth: (int?)rnd.Next(1, 10),
                                    medicalAccountMaxNumberShares: (int?)rnd.Next(2000, 3000),
                                    medicalAccountValue: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.2) * 10000, 2),
                                    interestAccountContribution: (decimal)Math.Round((double)contribution * 0.75, 2),
                                    interestAccountValue: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.2) * 10000, 2),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = "Sickness Interest Only";

                            var product = products
                                    .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            decimal contribution = (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 100, 2);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"HI{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    contribution: contribution,
                                    contributionFrequency: ContributionFrequencyType.Monthly,
                                    interestAccountContribution: contribution,
                                    interestAccountValue: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.2) * 10000, 2),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };

                        for (int i = 1; i <= _numberOfLiabilityDataPoints; i++)
                        {
                            orderNumber++;
                            var productReference = "Holloway Sickness Insurance Type 2";

                            var product = products
                                    .SingleOrDefault(p => p.Reference == productReference);

                            if (product == null)
                            {
                                continue;
                            }

                            var inputDataSet = await GetInputDataSetAsync(bod, product);

                            var dob1 = new DateTime(year: rnd.Next(1955, 1980), month: rnd.Next(1, 12), day: 1);
                            var genderLife1 = rnd.NextDouble() < 0.5 ? GenderType.Male : GenderType.Female;
                            genderLife1 = i == 1 ? GenderType.Male : i == 2 ? GenderType.Female : genderLife1;

                            var doe = new DateTime(year: rnd.Next(2000, 2010), month: rnd.Next(1, 12), day: 1);

                            decimal contribution = (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.5) * 100, 2);

                            decimal sicknessBenefit = (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.3) * 100, 0);

                            var dataItem = new LifeLiabilitiesInputDataSetItemQuery(workspaceId: _workspaceId,
                                    inputDataSetId: inputDataSet.Id,
                                    id: i,
                                    reference: $"HS{rnd.Next(10000, 999999)}",
                                    productId: product.Id,
                                    productType: product.Type,
                                    productReference: product.Reference,
                                    dateOfEntry: doe,
                                    dateOfBirthLife1: dob1,
                                    genderLife1: genderLife1,
                                    dateOfBirthLife2: null,
                                    genderLife2: null,
                                    contribution: contribution,
                                    contributionFrequency: ContributionFrequencyType.Monthly,
                                    contributionCeaseDate: dob1.AddYears(70),
                                    sicknessBenefit: sicknessBenefit,
                                    sicknessBenefitCoveredByMedicalAccount: (decimal?)Math.Round(sicknessBenefit * 0.7m, 2),
                                    medicalAccountValue: (decimal?)Math.Round(Math.Min(rnd.NextDouble(), 0.2) * 10000, 2),
                                    interestAccountValue: (decimal)Math.Round(Math.Min(rnd.NextDouble(), 0.2) * 10000, 2),
                                    numberOfPolicies: rnd.Next(10, 100),
                                    orderNumber: orderNumber);

                            AddToInputDataSet(inputDataSet, dataItem);
                        };
                    }

                    await StoreInputDataSetItems();

                    await CreateLifeLiabilitiesBodImportFile(bod: bod);
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Information($"CreateLifeLiabilitiesInputDataSet error: {e.Message}");
            }
        }

        private async Task CreateLifeLiabilitiesScenariosAsync(Random rnd)
        {
            Log.Information("CreateLifeLiabilitiesScenariosAsync.Start");

            try
            {
                if (_context.LifeLiabilitiesScenarios.Any())
                {
                    Log.Information("CreateLifeLiabilitiesScenariosAsync.Data");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles.Where(r => r.WorkspaceId == _workspaceId).ToListAsync();

                foreach (var reportingCycle in reportingCycles.OrderBy(r => r.EffectiveDate))
                {
                    reportingCycle.AddLifeLiabilitiesScenario(new LifeLiabilitiesScenario(workspaceId: _workspaceId,
                       reference: "Base",
                       performChangeInBasisProjections: !string.IsNullOrEmpty(reportingCycle.ChangeInBasisReportingCycleReference),
                       changeInBasisScenarioReference: !string.IsNullOrEmpty(reportingCycle.ChangeInBasisReportingCycleReference) ? "Base" : "",
                       modifier: "Seed"));

                    reportingCycle.AddLifeLiabilitiesScenario(new LifeLiabilitiesScenario(workspaceId: _workspaceId,
                        reference: "Low Bonuses",
                        deltaBELScenarioReference: "Base",
                        modifier: "Seed"));

                    reportingCycle.AddLifeLiabilitiesScenario(new LifeLiabilitiesScenario(workspaceId: _workspaceId,
                       reference: "Medium Bonuses",
                       modifier: "Seed"));

                    reportingCycle.AddLifeLiabilitiesScenario(new LifeLiabilitiesScenario(workspaceId: _workspaceId,
                       reference: "High Bonuses",
                       deltaBELScenarioReference: "Base",
                       modifier: "Seed"));

                    await _context.BulkSaveChangesAsync();
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateLifeLiabilitiesScenariosAsync");
            }

            Log.Information("CreateLifeLiabilitiesScenariosAsync.End");
        }

        private MortalityTable GenerateMortalityTable(Random rnd, string reference)
        {
            var mortalityTable = new MortalityTable(workspaceId: _workspaceId, reference: reference, modifier: SystemUsers.Seed);

            decimal rate = Math.Round((decimal)rnd.Next(1, 5) / 10000, 6);
            for (int age = 0; age <= 120; age++)
            {
                var rateIncrease = Math.Round((decimal)rnd.Next(1, 3) / 1000000, 6);
                if (age <= 16)
                {
                    mortalityTable.AddItem(new MortalityTableItem(workspaceId: _workspaceId, age: age, ultimateRate: 0, modifier: SystemUsers.Seed));
                }
                else
                {
                    if (age > 35)
                    {
                        rateIncrease = Math.Round((decimal)rnd.Next(40, 60) / 1000000, 6);
                    }
                    if (age > 65)
                    {
                        rateIncrease = Math.Round((decimal)rnd.Next(60, 80) / 1000000, 6);
                    }
                    else if (age > 75)
                    {
                        rateIncrease = Math.Round((decimal)rnd.Next(80, 120) / 1000000, 6); ;
                    }

                    rate = Math.Round(Math.Min(rate + Math.Min((decimal)rnd.NextDouble(), rateIncrease), 0.9m), 6);
                    mortalityTable.AddItem(new MortalityTableItem(workspaceId: _workspaceId, age: age, ultimateRate: rate, modifier: SystemUsers.Seed));
                }
            }

            return mortalityTable;
        }

        private async Task CreateMortalityTablesAsync(Random rnd)
        {
            Log.Information("CreateMortalityTablesAsync.Start");

            try
            {
                if (_context.MortalityTables.Any())
                {
                    Log.Information("CreateMortalityTablesAsync.Data   already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles.Where(r => r.WorkspaceId == _workspaceId).ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddMortalityTable(GenerateMortalityTable(rnd: rnd, reference: "Standard Male"));
                    reportingCycle.AddMortalityTable(GenerateMortalityTable(rnd: rnd, reference: "Standard Female"));
                    reportingCycle.AddMortalityTable(GenerateMortalityTable(rnd: rnd, reference: "Stressed Male"));
                    reportingCycle.AddMortalityTable(GenerateMortalityTable(rnd: rnd, reference: "Stressed Female"));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateMortalityTablesAsync");
            }

            Log.Information("CreateMortalityTablesAsync.End");
        }

        private SicknessTable GenerateSicknessTable(Random rnd, string reference, SicknessTableType type)
        {
            var sicknessTable = new SicknessTable(workspaceId: _workspaceId, reference: reference, type: type, modifier: SystemUsers.Seed);

            for (int age = 17; age <= 120; age++)
            {
                sicknessTable.AddItem(new SicknessTableItem(workspaceId: _workspaceId, age: age, rate1: 0.1234m, rate2: 0.1004m, rate3: 0.2765m, rate4: 0.2456m, rate5: 0.3456m, modifier: SystemUsers.Seed));
            }

            return sicknessTable;
        }

        private SicknessTable GenerateDurationSicknessTable(Random rnd, string reference, SicknessTableType type)
        {
            var sicknessTable = new SicknessTable(workspaceId: _workspaceId, reference: reference, type: type, modifier: SystemUsers.Seed);

            for (int age = 17; age <= 120; age++)
            {
                sicknessTable.AddItem(new SicknessTableItem(workspaceId: _workspaceId, age: age, rate1: rnd.Next(0, 100), rate2: rnd.Next(0, 100), rate3: rnd.Next(0, 100), rate4: rnd.Next(0, 100), rate5: rnd.Next(0, 100), modifier: SystemUsers.Seed));
            }

            return sicknessTable;
        }

        private async Task CreateSicknessTablesAsync(Random rnd)
        {
            Log.Information("CreateSicknessTablesAsync.Start");

            try
            {
                if (_context.SicknessTables.Any())
                {
                    Log.Information("CreateSicknessTablesAsync.Data   already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles.Where(r => r.WorkspaceId == _workspaceId).ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddSicknessTable(GenerateSicknessTable(rnd: rnd, reference: "Standard", type: SicknessTableType.DurationSick));
                    reportingCycle.AddSicknessTable(GenerateSicknessTable(rnd: rnd, reference: "Stressed", type: SicknessTableType.DurationSick));
                    reportingCycle.AddSicknessTable(GenerateSicknessTable(rnd: rnd, reference: "Incidence 1 Week", type: SicknessTableType.DeferredPeriod1Week));
                    reportingCycle.AddSicknessTable(GenerateSicknessTable(rnd: rnd, reference: "Incidence 0 Weeks", type: SicknessTableType.DeferredPeriod0Weeks));
                    reportingCycle.AddSicknessTable(GenerateDurationSicknessTable(rnd: rnd, reference: "Duration 1 Week", type: SicknessTableType.DeferredPeriod1Week));
                    reportingCycle.AddSicknessTable(GenerateDurationSicknessTable(rnd: rnd, reference: "Duration 0 Week", type: SicknessTableType.DeferredPeriod0Weeks));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateSicknessTablesAsync");
            }

            Log.Information("CreateSicknessTablesAsync.End");
        }

        private YearVector GenerateExpenseInflationRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.ExpenseInflationRates, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(5, 10);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? (decimal)Math.Min(Math.Round(rnd.NextDouble() / 50, 4), 0.05)
                    : (decimal)Math.Min(Math.Round((double)rate + Math.Min(Math.Round(rnd.NextDouble() / 50, 4), 0.01), 4), 0.1);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: k, rate: rate, modifier: SystemUsers.Seed));
            }

            return YearVectorRates;
        }

        private YearVector GeneratePolicyDurationLapseRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PolicyDurationLapseRates, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(5, 10);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? (decimal)Math.Min(Math.Round(rnd.NextDouble(), 4), 0.1)
                    : (decimal)Math.Round((double)rate * Math.Max(rnd.NextDouble(), 0.75), 4);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: k, rate: rate, modifier: SystemUsers.Seed));
            }

            return YearVectorRates;
        }

        private YearVector GeneratePartialWithdrawalRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PartialWithdrawalRates, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(5, 10);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? 0.1m
                    : Math.Max(rate - 0.01m, 0.02m);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: k, rate: rate, modifier: SystemUsers.Seed));

                if (rate <= 0.02m)
                {
                    break;
                }
            }

            return YearVectorRates;
        }

        private YearVector GenerateRegularBonusRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.RegularBonusRatesTable, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(1, 4);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? (decimal)Math.Round(rnd.Next(5) / 100.0, 2)
                    : (decimal)Math.Round((double)rate - 0.01, 2);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: 2017 + k, rate: rate, modifier: SystemUsers.Seed));
            }

            return YearVectorRates;
        }

        private YearVector GenerateTerminalBonusRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.TerminalBonusRates, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(20, 40);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? (decimal)Math.Min(Math.Round(rnd.NextDouble() / 50, 4), 0.1)
                    : (decimal)Math.Round((double)rate + Math.Min(Math.Round(rnd.NextDouble() / 50, 4), 0.1), 4);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: k, rate: rate, modifier: SystemUsers.Seed));
            }

            return YearVectorRates;
        }

        private YearVector GenerateMedicalAccountBonusRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MedicalAccountBonusRatesTable, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(1, 4);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? (decimal)Math.Round(rnd.Next(3, 5) / 100.0, 2)
                    : (decimal)Math.Max(Math.Round((double)rate - 0.01, 2), 0.01);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: 2017 + k, rate: rate, modifier: SystemUsers.Seed));
            }

            return YearVectorRates;
        }

        private YearVector GenerateMVARates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MVARates, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(20, 40);
            decimal rate = 1;
            for (int k = 1; k <= numKeys; k++)
            {
                rate = k == 1
                    ? (decimal)Math.Min(Math.Round(rnd.NextDouble() / 50, 4), 0.1)
                    : (decimal)Math.Max(Math.Round((double)rate - Math.Min(Math.Round(rnd.NextDouble() / 500, 4), 0.001), 4), 0);

                YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: k, rate: rate, modifier: SystemUsers.Seed));

                if (rate <= 0.1m)
                {
                    break;
                }
            }

            return YearVectorRates;
        }

        private YearVector GenerateMVAFreeDatesLapseRates(Random rnd, string reference)
        {
            var YearVectorRates = new YearVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MVAFreeDatesLapseRates, reference: reference, modifier: SystemUsers.Seed);
            YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: 10, rate: 0.2m, modifier: SystemUsers.Seed));
            YearVectorRates.AddRate(new YearVectorRate(workspaceId: _workspaceId, year: 15, rate: 0.15m, modifier: SystemUsers.Seed));

            return YearVectorRates;
        }

        private async Task CreateYearVectorRatesAsync(Random rnd)
        {
            Log.Information("CreateYearVectorRatesAsync.Start");

            try
            {
                if (_context.YearVectors.Any())
                {
                    Log.Information("CreateYearVectorRatesAsync.Data   already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles.Where(r => r.WorkspaceId == _workspaceId).ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddYearVector(GenerateExpenseInflationRates(rnd: rnd, reference: "Expense Inflation Rates"));

                    reportingCycle.AddYearVector(GeneratePolicyDurationLapseRates(rnd: rnd, reference: "Standard Policy Duration Lapse Rates"));
                    reportingCycle.AddYearVector(GeneratePolicyDurationLapseRates(rnd: rnd, reference: "Enhanced Policy Duration Lapse Rates"));

                    reportingCycle.AddYearVector(GeneratePartialWithdrawalRates(rnd: rnd, reference: "Standard Partial Withdrawal Rates"));
                    reportingCycle.AddYearVector(GeneratePartialWithdrawalRates(rnd: rnd, reference: "Enhanced Partial Withdrawal Rates"));

                    reportingCycle.AddYearVector(GenerateMVARates(rnd: rnd, reference: "Standard MVA Rates"));
                    reportingCycle.AddYearVector(GenerateMVARates(rnd: rnd, reference: "Enhanced MVA Rates"));

                    reportingCycle.AddYearVector(GenerateRegularBonusRates(rnd: rnd, reference: "Standard Regular Bonus Rates"));
                    reportingCycle.AddYearVector(GenerateRegularBonusRates(rnd: rnd, reference: "Enhanced Regular Bonus Rates"));

                    reportingCycle.AddYearVector(GenerateTerminalBonusRates(rnd: rnd, reference: "Standard Terminal Bonus Rates"));
                    reportingCycle.AddYearVector(GenerateTerminalBonusRates(rnd: rnd, reference: "Enhanced Terminal Bonus Rates"));

                    reportingCycle.AddYearVector(GenerateMVAFreeDatesLapseRates(rnd: rnd, reference: "Standard MVA Free Dates Lapse Rates"));

                    reportingCycle.AddYearVector(GenerateMedicalAccountBonusRates(rnd: rnd, reference: "Standard Medical Account Bonus Rates"));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateYearVectorRatesAsync");
            }

            Log.Information("CreateYearVectorRatesAsync.End");
        }

        private AgeVector GenerateAgeRelatedLapseRates(Random rnd, string reference)
        {
            var ageVectorRates = new AgeVector(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AgeRelatedLapseRates, reference: reference, modifier: SystemUsers.Seed);

            var numKeys = rnd.Next(5, 10);
            for (int k = 0; k <= numKeys; k++)
            {
                ageVectorRates.AddRate(new AgeVectorRate(workspaceId: _workspaceId, age: k * 5, rate: (decimal)Math.Min(Math.Round(rnd.NextDouble(), 4), 0.1), modifier: SystemUsers.Seed));
            }

            return ageVectorRates;
        }

        private async Task CreateAgeVectorRatesAsync(Random rnd)
        {
            Log.Information("CreateAgeVectorRatesAsync.Start");

            try
            {
                if (_context.AgeVectors.Any())
                {
                    Log.Information("CreateAgeVectorRatesAsync.Data   already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles.Where(r => r.WorkspaceId == _workspaceId).ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddAgeVector(GenerateAgeRelatedLapseRates(rnd: rnd, reference: "Standard Age Related Lapse Rates"));
                    reportingCycle.AddAgeVector(GenerateAgeRelatedLapseRates(rnd: rnd, reference: "Enhanced Age Related Lapse Rates"));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateAgeVectorRatesAsync");
            }

            Log.Information("CreateAgeVectorRatesAsync.End");
        }

        private YieldCurve GenerateYieldCurve(Random rnd, string reference)
        {
            var yieldCurve = new YieldCurve(workspaceId: _workspaceId, reference: reference, modifier: SystemUsers.Seed);

            decimal spotRate = 1;
            for (int year = 1; year <= 150; year++)
            {
                spotRate = year == 1
                    ? (decimal)Math.Min(Math.Round(rnd.NextDouble(), 4), 0.05)
                    : (decimal)Math.Round((double)spotRate + 0.0005, 4);

                yieldCurve.AddSpotRate(new YieldCurveRate(workspaceId: _workspaceId, year: year, spotRate: spotRate, modifier: SystemUsers.Seed));
            }

            return yieldCurve;
        }

        private async Task CreateYieldCurvesAsync(Random rnd)
        {
            Log.Information("CreateYieldCurvesAsync.Start");

            try
            {
                if (_context.YieldCurves.Any())
                {
                    Log.Information("CreateYieldCurvesAsync.Data already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles.Where(r => r.WorkspaceId == _workspaceId).ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddYieldCurve(GenerateYieldCurve(rnd: rnd, reference: "Standard Yield Curve"));
                    reportingCycle.AddYieldCurve(GenerateYieldCurve(rnd: rnd, reference: "Adjusted Yield Curve"));
                    reportingCycle.AddYieldCurve(GenerateYieldCurve(rnd: rnd, reference: "All Share"));
                    reportingCycle.AddYieldCurve(GenerateYieldCurve(rnd: rnd, reference: "Mixed Equity"));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateYieldCurvesAsync");
            }

            Log.Information("CreateYieldCurvesAsync.End");
        }

        private async Task CreateLifeLiabilitiesAssumptionsAsync(Random rnd)
        {
            Log.Information("CreateLifeLiabilitiesAssumptionsAsync.Start");

            try
            {
                if (_context.LifeLiabilitiesAssumptions.Any())
                {
                    Log.Information("CreateLifeLiabilitiesAssumptionsAsync.Data   already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles
                    .Where(r => r.WorkspaceId == _workspaceId)
                    .ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    var regulatoryFunds = await _context.SIIRegulatoryFunds
                        .Include(r => r.ReportingEntity)
                        .Where(r => r.ReportingEntity.ReportingCycleId == reportingCycle.Id)
                        .ToListAsync();

                    var products = await _context.SIIProducts
                        .Include(r => r.RegulatoryFund)
                            .ThenInclude(r => r.ReportingEntity)
                        .Where(r => r.RegulatoryFund.ReportingEntity.ReportingCycleId == reportingCycle.Id)
                        .ToListAsync();

                    var investmentFunds = await _context.SIIInvestmentFunds
                        .Include(r => r.RegulatoryFund)
                            .ThenInclude(r => r.ReportingEntity)
                         .Where(r => r.RegulatoryFund.ReportingEntity.ReportingCycleId == reportingCycle.Id)
                        .ToListAsync();

                    var mortalityTables = await _context.MortalityTables
                    .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id)
                    .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityTable, gender: GenderType.Male, value: mortalityTables[rnd.Next(0, mortalityTables.Count - 1)].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityTable, gender: GenderType.Female, value: mortalityTables[rnd.Next(0, mortalityTables.Count - 1)].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityTable, gender: GenderType.Male, value: mortalityTables[rnd.Next(0, mortalityTables.Count - 1)].Id.ToString(), productReference: products[0].Reference, modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityTable, gender: GenderType.Male, value: mortalityTables[rnd.Next(0, mortalityTables.Count - 1)].Id.ToString(), productReference: products[1].Reference, modifier: SystemUsers.Seed));

                    //expenses
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyInitialExpense, value: "500", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyInitialExpense, regulatoryFundReference: regulatoryFunds[0].Reference, value: "1000", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyInitialExpense, productType: ProductType.CNP, value: "600", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyInitialExpense, productReference: products[0].Reference, value: "700", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyAnnualExpense, value: "250", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyAnnualExpense, productType: ProductType.CNP, value: "350", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyAnnualExpense, productReference: products[0].Reference, value: "350", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyAnnualExpense, productGroup: "CWP", value: "450", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PerPolicyMaturityExpense, productType: ProductType.UL, value: "300", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PercentageContributionExpense, productType: ProductType.CWP, value: "0.1", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.ExpenseFundPercentage, value: "0.005", productType: ProductType.CWP, modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.ExpenseFundPercentage, value: "0.01", productType: ProductType.UL, modifier: SystemUsers.Seed));

                    var expenseInflationRates = await _context.YearVectors
                        .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.ExpenseInflationRates)
                        .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.ExpenseInflationRates, value: expenseInflationRates[0].Id.ToString(), modifier: SystemUsers.Seed));

                    //decrements
                    var policyDurationLapseRates = await _context.YearVectors
                        .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.PolicyDurationLapseRates)
                        .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PolicyDurationLapseRates, value: policyDurationLapseRates[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PolicyDurationLapseRates, value: policyDurationLapseRates[1].Id.ToString(), productReference: products[0].Reference, modifier: SystemUsers.Seed));

                    //economic
                    var yieldCurves = await _context.YieldCurves
                        .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id)
                        .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.DiscountingYieldCurve, value: yieldCurves[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.DiscountingYieldCurve, value: yieldCurves[1].Id.ToString(), productReference: products[0].Reference, modifier: SystemUsers.Seed));

                    //annuities
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AnnuityMarriedPercentage, productType: ProductType.Annuity, value: "0.9", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AnnuityLife2AgeDifference, productType: ProductType.Annuity, gender: GenderType.Male, value: "3", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AnnuityLife2AgeDifference, productType: ProductType.Annuity, gender: GenderType.Female, value: "-3", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AnnuitySpouseBenefitPercentage, productType: ProductType.Annuity, value: "0.5", modifier: SystemUsers.Seed));

                    var regularBonusRates = await _context.YearVectors
                         .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id
                           && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.RegularBonusRatesTable)
                         .ToListAsync();

                    var terminalBonusRates = await _context.YearVectors
                          .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id
                            && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.TerminalBonusRates)
                          .ToListAsync();

                    // Data
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.BonusOnDataToDate, productType: ProductType.CWP, value: "31/12/2016", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.BonusOnDataToDate, value: "30/06/2017", modifier: SystemUsers.Seed));

                    //With Profits

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.TerminalBonusRates, value: terminalBonusRates[0].Id.ToString(), modifier: SystemUsers.Seed));

                    var scenarios = await _context.LifeLiabilitiesScenarios
                         .Where(r => r.ReportingCycleId == reportingCycle.Id)
                         .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimRegularBonusRate, value: "0.01", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimRegularBonusRate, scenarioReference: scenarios[0].Reference, value: "0.02", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimRegularBonusRate, scenarioReference: scenarios[1].Reference, productType: ProductType.CWP, value: "0.03", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimRegularBonusRate, productType: ProductType.UWP, value: "0.04", modifier: SystemUsers.Seed));

                    //CWP
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.RegularBonusRatesTable, value: regularBonusRates[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AnnualAssetShareChargePercentage, value: "0.01", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AssetShareInvestmentReturnYieldCurve, value: yieldCurves[0].Id.ToString(), modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AssetShareStressTestPercentage, basis: LifeLiabilitiesStressBasisType.InterestRateUpStress,
                       value: "0.8", modifier: SystemUsers.Seed));

                    //UWP

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.ExpenseFundPercentage, value: "0.01", productReference: "Unitised With Profits", modifier: SystemUsers.Seed));

                    var partialWithdrawalRates = await _context.YearVectors
                       .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.PartialWithdrawalRates)
                       .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PartialWithdrawalRates, value: partialWithdrawalRates[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.PartialWithdrawalRates, value: partialWithdrawalRates[1].Id.ToString(), productReference: "Unitised With Profits", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.RegularBonusRatesYieldCurve, value: yieldCurves[0].Id.ToString(), productReference: "Unitised With Profits", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.TerminalBonusRates, value: terminalBonusRates[1].Id.ToString(), productReference: "Unitised With Profits", modifier: SystemUsers.Seed));

                    var mvaRates = await _context.YearVectors
                     .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id
                       && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.MVARates)
                     .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MVARates, value: mvaRates[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MVARates, value: mvaRates[1].Id.ToString(), productReference: "Unitised With Profits", modifier: SystemUsers.Seed));

                    var mvaFreeDatesLapseRates = await _context.YearVectors
                     .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id
                       && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.MVAFreeDatesLapseRates)
                     .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MVAFreeDatesLapseRates, value: mvaFreeDatesLapseRates[0].Id.ToString(), productReference: "Unitised With Profits", modifier: SystemUsers.Seed));

                    // COG
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.COGEquityBackingRatioPercentage, value: "0.23", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.COGDividendYieldPercentage, value: "0.03", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.COGVolatilityPercentage, value: "0.1", modifier: SystemUsers.Seed));

                    // UL
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityChargeTable, gender: GenderType.Male, value: mortalityTables[rnd.Next(0, mortalityTables.Count - 1)].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityChargeTable, gender: GenderType.Female, value: mortalityTables[rnd.Next(0, mortalityTables.Count - 1)].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MortalityChargeAgeAdj, gender: GenderType.Male, value: "-3", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.FundInvestmentReturns,
                        value: yieldCurves[2].Id.ToString(), investmentFundReference: investmentFunds[0].Reference, modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.FundInvestmentReturns,
                         value: yieldCurves[3].Id.ToString(), modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.UnitPrice,
                       value: "1.485", investmentFundReference: investmentFunds[0].Reference, modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.FundValueStressTestPercentage, basis: LifeLiabilitiesStressBasisType.InterestRateUpStress,
                        value: "0.8", investmentFundReference: investmentFunds[0].Reference, modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.FundValueStressTestPercentage, basis: LifeLiabilitiesStressBasisType.InterestRateUpStress,
                        value: "0.9", investmentFundReference: investmentFunds[1].Reference, modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.FundValueStressTestPercentage, basis: LifeLiabilitiesStressBasisType.InterestRateUpStress,
                        value: "0.95", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.FundValueStressTestPercentage, basis: LifeLiabilitiesStressBasisType.EquityStress,
                        value: "0.75", modifier: SystemUsers.Seed));

                    // PHI

                    var sicknessTablesDefferedPeriod1 = await _context.SicknessTables
                      .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.Type == SicknessTableType.DeferredPeriod1Week)
                      .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessIncidenceTable, productReference: "PHI",
                        value: sicknessTablesDefferedPeriod1.First(x => x.Reference.Contains("Incidence")).Id.ToString(),
                        modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessDurationTable, productReference: "PHI",
                        value: sicknessTablesDefferedPeriod1.First(x => x.Reference.Contains("Duration")).Id.ToString(),
                        modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessIncidenceRateMultiplier, productReference: "PHI", value: "0.7", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessDurationRateMultiplier, productReference: "PHI", value: "0.7", modifier: SystemUsers.Seed));

                    // Holloway

                    var sicknessTablesIncidenceDuration = await _context.SicknessTables
                       .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.Type == SicknessTableType.DurationSick)
                       .ToListAsync();

                    var sicknessTablesDefferedPeriod0 = await _context.SicknessTables
                      .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.Type == SicknessTableType.DeferredPeriod0Weeks)
                      .ToListAsync();

                    var ageRelatedLapseRates = await _context.AgeVectors
                      .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.AgeRelatedLapseRates)
                      .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AgeRelatedLapseRates, value: ageRelatedLapseRates[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.AgeRelatedLapseRates, value: ageRelatedLapseRates[1].Id.ToString(), productReference: "Holloway Sickness Insurance Type 1", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessIncidenceDurationTable, value: sicknessTablesIncidenceDuration[rnd.Next(0, sicknessTablesIncidenceDuration.Count - 1)].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessIncidenceDurationRateMultiplier, value: "0.7", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessIncidenceTable, productType: ProductType.HollowaySicknessType2,
                      value: sicknessTablesDefferedPeriod0.First(x => x.Reference.Contains("Incidence")).Id.ToString(),
                      modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.SicknessDurationTable, productType: ProductType.HollowaySicknessType2,
                        value: sicknessTablesDefferedPeriod0.First(x => x.Reference.Contains("Duration")).Id.ToString(),
                        modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimInterestAccountBonusRate, value: "0.01", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimInterestAccountBonusRate, scenarioReference: scenarios[1].Reference, value: "0.02", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterestAccountBonusRatesYieldCurve, value: yieldCurves[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterestAccountAMCPercentage, value: "0.01", productType: ProductType.HollowaySicknessType1, modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimMedicalAccountBonusRate, value: "0.12", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.InterimMedicalAccountBonusRate, scenarioReference: scenarios[0].Reference, value: "0.24", modifier: SystemUsers.Seed));

                    var medicalAccountBonusRates = await _context.YearVectors
                        .Where(r => r.WorkspaceId == _workspaceId && r.ReportingCycleId == reportingCycle.Id
                          && r.AssumptionIdentifier == LifeLiabilitiesAssumptionIdentifier.MedicalAccountBonusRatesTable)
                        .ToListAsync();

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MedicalAccountBonusRatesTable, value: medicalAccountBonusRates[0].Id.ToString(), modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MedicalAccountBonusRatesYieldCurve, value: yieldCurves[0].Id.ToString(), productType: ProductType.HollowaySicknessType2, modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MedicalAccountTransferPercentage, value: "0.9", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MorbidityIncidenceDurationStressTestPercentage, value: "1.6", modifier: SystemUsers.Seed));

                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MedicalAccountContributionAllocationPercentage, value: "0.75", modifier: SystemUsers.Seed));
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.MedicalAccountChargePercentage, value: "1.25", modifier: SystemUsers.Seed));

                    // adjustments
                    reportingCycle.AddLifeLiabilitiesAssumption(new LifeLiabilitiesAssumption(workspaceId: _workspaceId, assumptionIdentifier: LifeLiabilitiesAssumptionIdentifier.TaxAdjustmentPercentage, value: "0.2", productReference: "Unitised With Profits", modifier: SystemUsers.Seed));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateLifeLiabilitiesAssumptionsAsync");
            }

            Log.Information("CreateLifeLiabilitiesAssumptionsAsync.End");
        }

        private TaskStatusType CreateTaskStatus(Random rnd, ReportingCycle reportingCycle)
        {
            if (reportingCycle.Status == ReportingCycleStatusType.Finalised)
            {
                return TaskStatusType.Completed;
            }

            var taskStatus = TaskStatusType.Completed;

            var random = rnd.NextDouble();

            if (random < 0.1)
            {
                return TaskStatusType.Failed;
            }

            if (random < 0.2)
            {
                return TaskStatusType.Warnings;
            }

            return taskStatus;
        }

        private async Task CreateLifeLiabilitiesTasksAsync(Random rnd)
        {
            Log.Information("CreateLifeLiabilitiesTasksAsync.Start SolvencyII");

            try
            {
                if (_context.LifeLiabilitiesTasks.Any())
                {
                    Log.Information("CreateLifeLiabilitiesTasksAsync.Data  SII already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles
                    .Include(r => r.LifeLiabilitiesScenarios)
                    .Include(r => r.ReportingEntities)
                        .ThenInclude(r => r.RegulatoryFunds)
                            .ThenInclude(r => r.LifeLiabilitiesBods)
                    .Where(r => r.WorkspaceId == _workspaceId)
                    .ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    foreach (var scenario in reportingCycle.LifeLiabilitiesScenarios)
                    {
                        foreach (var reportingEntity in reportingCycle.ReportingEntities)
                        {
                            foreach (var regulatoryFund in reportingEntity.RegulatoryFunds)
                            {
                                foreach (var bod in regulatoryFund.LifeLiabilitiesBods)
                                {
                                    reportingCycle.AddLifeLiabilitiesTask(new LifeLiabilitiesTask(workspaceId: _workspaceId,
                                        scenarioId: scenario.Id,
                                        bodId: bod.Id,
                                        executeProjectionsInParallel: false,
                                        includeCashflows: true,
                                        includeDetailedCashflows: true,
                                        modifier: "Seed"));
                                }
                            }
                        }
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateLifeLiabilitiesTasksAsync SolvencyII");
            }

            Log.Information("CreateLifeLiabilitiesTasksAsync.End SolvencyII");
        }

        private async Task CreateLifeLiabilitiesCashflowProjectionTaskResultsAsync(Random rnd,
            ReportingCycle reportingCycle,
            LifeLiabilitiesTask task,
            LifeLiabilitiesTaskType taskType,
            LifeLiabilitiesStressBasisType? basis,
            LifeLiabilitiesChangeInBasisStepType? changeInBasisStep)
        {
            try
            {
                for (int productId = 1; productId <= 2; productId++)
                {
                    LifeLiabilitiesSelectedResultsSet selectedResultsSet = new LifeLiabilitiesSelectedResultsSet(workspaceId: _workspaceId,
                        taskType: taskType,
                        basis: basis,
                        changeInBasisStep: changeInBasisStep,
                        productId: productId,
                        belPreCOG: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        cog: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        bel: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        modifier: "seed");

                    task.AddSelectedResultsSet(selectedResultsSet);
                    await _context.BulkSaveChangesAsync(); // to get selectedResultsSet

                    var selectedResultsSetItems = new List<LifeLiabilitiesSelectedResultsSetItemQuery>();

                    for (int id = 1; id <= _numberOfLiabilityDataPoints; id++)
                    {
                        selectedResultsSetItems.Add(new LifeLiabilitiesSelectedResultsSetItemQuery(
                            workspaceId: _workspaceId,
                            selectedResultsSetId: selectedResultsSet.Id,
                            reportingCycleId: task.ReportingCycleId,
                            taskId: task.Id,
                            taskType: taskType,
                            scenarioId: task.ScenarioId,
                            scenarioReference: task.Scenario.Reference,
                            bodId: task.BodId,
                            bodReference: task.Bod.Reference,
                            basis: basis,
                            changeInBasisStep: changeInBasisStep,
                            id: id,
                            note: "Note",
                            reference: $"Reference {id}",
                            inputDataSetItemId: id,
                            productId: productId,
                            productReference: "Product",
                            productGroup: "Group",
                            productType: ProductType.CNP,
                            ageLife1: rnd.Next(20, 40),
                            ageLife2: rnd.Next(20, 40),
                            genderLife1: GenderType.Male,
                            genderLife2: GenderType.Female,
                            contribution: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            sumAssured: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            assetShare: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            annuity: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            numberOfPolicies: rnd.Next(1, 40),
                            fundValue: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            regularBonus: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            belPreCOG: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            cog: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            bel: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            orderNumber: id
                        ));
                    }

                    string json = JsonConvert.SerializeObject(selectedResultsSetItems);

                    await _storage.UploadAsync(containerName: _containerName,
                        fileStorageId: selectedResultsSet.UniqueId,
                        text: json);
                }

                for (int id = 1; id <= _numberOfLiabilityDataPoints; id++)
                {
                    LifeLiabilitiesDetailedCashflowsSet detailedCashflowSet = new LifeLiabilitiesDetailedCashflowsSet(workspaceId: _workspaceId,
                        taskType: taskType,
                        basis: basis,
                        changeInBasisStep: changeInBasisStep,
                        productId: 1,
                        inputDataSetItemId: id,
                        inputDataSetItemReference: $"Policy {id}",
                        belPreCOG: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        cog: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        bel: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        modifier: "seed");

                    task.AddDetailedCashflowSet(detailedCashflowSet);
                    await _context.BulkSaveChangesAsync(); // to get detailedCashflowSet

                    var detailedCashflowSetItems = new List<LifeLiabilitiesDetailedCashflowsSetItemQuery>();

                    for (int t = 0; t <= 2; t++)
                    {
                        detailedCashflowSetItems.Add(new LifeLiabilitiesDetailedCashflowsSetItemQuery(workspaceId: _workspaceId,
                            detailedCashflowsSetId: detailedCashflowSet.Id,
                            reportingCycleId: task.ReportingCycleId,
                            id: t + 1,
                            reference: t.ToString(),
                            taskId: task.Id,
                            taskType: taskType,
                            bodId: task.BodId,
                            bodReference: task.Bod.Reference,
                            reportingEntityId: task.Bod.RegulatoryFund.ReportingEntityId,
                            reportingEntityReference: task.Bod.RegulatoryFund.ReportingEntity.Reference,
                            regulatoryFundId: task.Bod.RegulatoryFundId,
                            regulatoryFundReference: task.Bod.RegulatoryFund.Reference,
                            scenarioId: task.ScenarioId,
                            scenarioReference: task.Scenario.Reference,
                            productId: 1,
                            productReference: "Product 1",
                            inputDataSetItemId: id,
                            inputDataSetItemReference: $"Policy {id}",
                            basis: basis,
                            changeInBasisStep: changeInBasisStep)
                        {
                            WorkspaceId = _workspaceId,
                            Date = DateTime.UtcNow,
                            PolicyDuration = 10,
                            AgeLife1 = 45,
                            AgeLife2 = 23,
                            ContributionsInForce = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            DeathBenefitsInForce = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            Contributions = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            DeathBenefits = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            BEL = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2)
                        });
                    }

                    List<LifeLiabilitiesDetailedInvestmentFundAllocationCashflowsSetQuery> detailedInvestmentAllocationCashflowSets = null;

                    if (id == 2)
                    {
                        detailedInvestmentAllocationCashflowSets = new List<LifeLiabilitiesDetailedInvestmentFundAllocationCashflowsSetQuery>();

                        for (int ivId = 1; ivId <= 2; ivId++)
                        {
                            var detailedInvestmentFundAllocationCashflowSetItems = new List<LifeLiabilitiesDetailedInvestmentFundAllocationCashflowsSetItemQuery>();

                            for (int t = 0; t <= 2; t++)
                            {
                                detailedInvestmentFundAllocationCashflowSetItems.Add(new LifeLiabilitiesDetailedInvestmentFundAllocationCashflowsSetItemQuery(workspaceId: _workspaceId,
                                    detailedInvestmentFundAllocationCashflowsSetId: ivId,
                                    reportingCycleId: task.ReportingCycleId,
                                    id: t + 1,
                                    reference: t.ToString(),
                                    taskId: task.Id,
                                    taskType: taskType,
                                    bodId: task.BodId,
                                    bodReference: task.Bod.Reference,
                                    reportingEntityId: task.Bod.RegulatoryFund.ReportingEntityId,
                                    reportingEntityReference: task.Bod.RegulatoryFund.ReportingEntity.Reference,
                                    regulatoryFundId: task.Bod.RegulatoryFundId,
                                    regulatoryFundReference: task.Bod.RegulatoryFund.Reference,
                                    scenarioId: task.ScenarioId,
                                    scenarioReference: task.Scenario.Reference,
                                    productId: 1,
                                    productReference: "Product 1",
                                    inputDataSetItemId: id,
                                    inputDataSetItemReference: $"Policy {id}",
                                    investmentFundId: ivId,
                                    investmentFundReference: $"Investment Fund {ivId}",
                                    basis: basis,
                                    changeInBasisStep: changeInBasisStep)
                                {
                                    FundValueUndecremented = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2)
                                });
                            }

                            var detailedInvestmentFundAllocationCashflowSet = new LifeLiabilitiesDetailedInvestmentFundAllocationCashflowsSetQuery(workspaceId: _workspaceId,
                                   detailedCashflowsSetId: detailedCashflowSet.Id,
                                   id: ivId,
                                   reference: $"Investment Fund {ivId}",
                                   investmentFundId: ivId,
                                   investmentFundReference: $"Investment Fund {ivId}",
                                   items: detailedInvestmentFundAllocationCashflowSetItems);

                            detailedInvestmentAllocationCashflowSets.Add(detailedInvestmentFundAllocationCashflowSet);
                        }
                    }

                    var detailedCashflowSetContents = new LifeLiabilitiesDetailedCashflowsSetContentsQuery(workspaceId: detailedCashflowSet.WorkspaceId,
                        id: detailedCashflowSet.Id,
                        reference: detailedCashflowSet.Reference,
                        items: detailedCashflowSetItems,
                        investmentFundAllocations: detailedInvestmentAllocationCashflowSets);

                    var json = JsonConvert.SerializeObject(detailedCashflowSetContents);

                    await _storage.UploadAsync(containerName: _containerName,
                        fileStorageId: detailedCashflowSet.UniqueId,
                        text: json);

                    task.AddDetailedCashflowSet(detailedCashflowSet);
                }

                for (int productId = 1; productId <= 2; productId++)
                {
                    LifeLiabilitiesCashflowsSet cashflowSet = new LifeLiabilitiesCashflowsSet(workspaceId: _workspaceId,
                        taskType: taskType,
                        basis: basis,
                        changeInBasisStep: changeInBasisStep,
                        productId: productId,
                        contributionsInForce: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        sumAssuredInForce: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        belPreCOG: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        cog: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                        bel: (decimal?)Math.Round(rnd.NextDouble() * 10000, 2));

                    task.AddCashflowSet(cashflowSet);
                    await _context.BulkSaveChangesAsync(); // to get cashflowsSetId

                    var cashflowSetItems = new List<LifeLiabilitiesCashflowsSetItemQuery>();

                    for (int t = 0; t <= 2; t++)
                    {
                        var cashflowSetItem = new LifeLiabilitiesCashflowsSetItemQuery(workspaceId: _workspaceId,
                            cashflowsSetId: cashflowSet.Id,
                            reportingCycleId: task.ReportingCycleId,
                            id: t + 1,
                            reference: t.ToString(),
                            taskId: task.Id,
                            taskType: taskType,
                            bodId: task.BodId,
                            bodReference: task.Bod.Reference,
                            reportingEntityId: task.Bod.RegulatoryFund.ReportingEntityId,
                            reportingEntityReference: task.Bod.RegulatoryFund.ReportingEntity.Reference,
                            regulatoryFundId: task.Bod.RegulatoryFundId,
                            regulatoryFundReference: task.Bod.RegulatoryFund.Reference,
                            scenarioId: task.ScenarioId,
                            scenarioReference: task.Scenario.Reference,
                            productId: productId,
                            productReference: $"Product {productId}",
                            basis: basis,
                            changeInBasisStep: changeInBasisStep)
                        {
                            ContributionsInForce = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            SumAssuredInForce = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            Contributions = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            DeathBenefits = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            BELPreCOG = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            COG = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2),
                            BEL = (decimal?)Math.Round(rnd.NextDouble() * 10000, 2)
                        };

                        cashflowSetItems.Add(cashflowSetItem);
                    }

                    var json = JsonConvert.SerializeObject(cashflowSetItems);
                    await _storage.UploadAsync(containerName: _containerName, fileStorageId: cashflowSet.UniqueId, text: json);
                }
            }
            catch (Exception e)
            {
                Log.Information($"CreateLifeLiabilitiesCashflowProjectionTaskResults error - {e.Message}");
            }
        }

        private async Task CreateLifeLiabilitiesCashflowProjectionTaskSummaryResults(Random rnd,
            LifeLiabilitiesTask task)
        {
            try
            {
                LifeLiabilitiesSummaryResultsSet summaryResultsSet = new LifeLiabilitiesSummaryResultsSet(workspaceId: _workspaceId);

                task.AddSummaryResultsSet(summaryResultsSet);
                await _context.BulkSaveChangesAsync(); // to get summaryResultsSetId

                var summaryResultsSetItems = new List<LifeLiabilitiesSummaryResultsSetItemQuery>();

                int id = 0;
                for (int productId = 1; productId <= 2; productId++)
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        id++;

                        summaryResultsSetItems.Add(new LifeLiabilitiesSummaryResultsSetItemQuery
                        {
                            SummaryResultsSetId = summaryResultsSet.Id,
                            WorkspaceId = _workspaceId,
                            ReportingCycleId = task.ReportingCycleId,
                            Id = id,
                            Reference = Guid.NewGuid().ToString(),
                            TaskId = task.Id,
                            BodId = task.BodId,
                            BodReference = task.Bod.Reference,
                            ReportingEntityId = task.Bod.RegulatoryFund.ReportingEntityId,
                            ReportingEntityReference = task.Bod.RegulatoryFund.ReportingEntity.Reference,
                            RegulatoryFundId = task.Bod.RegulatoryFundId,
                            RegulatoryFundReference = task.Bod.RegulatoryFund.Reference,
                            ScenarioId = task.ScenarioId,
                            ScenarioReference = task.Scenario.Reference,
                            ProductType = ProductType.CNP,
                            ProductGroup = "Product Group",
                            ProductId = productId,
                            ProductReference = $"Product {productId}",
                            TaskType = LifeLiabilitiesTaskType.BELProjection,
                            Basis = i == 1
                                ? (LifeLiabilitiesStressBasisType?)LifeLiabilitiesStressBasisType.InterestRateDownStress
                                 : i == 2 ? (LifeLiabilitiesStressBasisType?)LifeLiabilitiesStressBasisType.InterestRateUpStress
                                     : (LifeLiabilitiesStressBasisType?)LifeLiabilitiesStressBasisType.Base,
                            ChangeInBasisStep = i == 1
                                ? (LifeLiabilitiesChangeInBasisStepType?)LifeLiabilitiesChangeInBasisStepType.ChangeInExpenses
                                 : i == 2 ? (LifeLiabilitiesChangeInBasisStepType?)LifeLiabilitiesChangeInBasisStepType.ChangeInModel
                                     : null,
                            NumberOfPolicies = rnd.Next(100),
                            Contributions = rnd.Next(10000),
                            SumAssured = rnd.Next(10000),
                            RegularBonus = rnd.Next(10000),
                            AssetShare = rnd.Next(10000),
                            FundValue = rnd.Next(10000),
                            Annuity = rnd.Next(10000),
                            BaseBEL = rnd.Next(10000),
                            BELPreCOG = rnd.Next(10000),
                            COG = rnd.Next(10000),
                            BEL = rnd.Next(10000),
                            DeltaToBaseBEL = rnd.Next(10000),
                            Capital = rnd.Next(10000)
                        });
                    }
                }

                var json = JsonConvert.SerializeObject(summaryResultsSetItems);
                await _storage.UploadAsync(containerName: _containerName,
                        fileStorageId: summaryResultsSet.UniqueId,
                        text: json);
            }
            catch (Exception e)
            {
                Log.Information($"CreateLifeLiabilitiesCashflowProjectionTaskResults error - {e.Message}");
            }
        }

        private async Task DoLiabilityCashflowProjectionTasks(Random rnd)
        {
            Log.Information("DoLiabilityCashflowProjectionTasks.Start SolvencyII");

            try
            {
                var reportingCycles = await _context.SIIReportingCycles
                   .Include(r => r.LifeLiabilitiesTasks)
                        .ThenInclude(r => r.Scenario)
                   .Include(r => r.LifeLiabilitiesTasks)
                        .ThenInclude(r => r.Bod)
                   .Include(r => r.LifeLiabilitiesTasks)
                        .ThenInclude(r => r.Bod)
                            .ThenInclude(t => t.InputDataSets)
                   .Where(r => r.WorkspaceId == _workspaceId)
                   .ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    int count = 0;
                    foreach (var task in reportingCycle.LifeLiabilitiesTasks)
                    {
                        count++;
                        if (count > 2)
                        {
                            continue;
                        }

                        if (task.Scenario.PerformBelProjection)
                        {
                            await CreateLifeLiabilitiesCashflowProjectionTaskResultsAsync(rnd: rnd,
                                 reportingCycle: reportingCycle,
                                 task: task,
                                 taskType: LifeLiabilitiesTaskType.BELProjection,
                                 changeInBasisStep: null,
                                 basis: LifeLiabilitiesStressBasisType.Base);
                        }

                        if (task.Scenario.PerformChangeInBasisProjections)
                        {
                            foreach (var basis in task.Bod.GetBelStressTestBases())
                            {
                                if (basis > LifeLiabilitiesStressBasisType.InterestRateDownStress)
                                {
                                    continue;
                                }

                                await CreateLifeLiabilitiesCashflowProjectionTaskResultsAsync(rnd: rnd,
                                   reportingCycle: reportingCycle,
                                   task: task,
                                   taskType: LifeLiabilitiesTaskType.BELStresses,
                                   changeInBasisStep: null,
                                   basis: basis);
                            }
                        }

                        if (task.Scenario.PerformChangeInBasisProjections)
                        {
                            foreach (var changeInBasisStep in task.Bod.GetChangeInBasesSteps())
                            {
                                if (changeInBasisStep > LifeLiabilitiesChangeInBasisStepType.ChangeInData)
                                {
                                    continue;
                                }

                                await CreateLifeLiabilitiesCashflowProjectionTaskResultsAsync(rnd: rnd,
                                   reportingCycle: reportingCycle,
                                   task: task,
                                   taskType: LifeLiabilitiesTaskType.ChangeInBasis,
                                   changeInBasisStep: changeInBasisStep,
                                   basis: LifeLiabilitiesStressBasisType.Base);
                            }
                        }

                        await CreateLifeLiabilitiesCashflowProjectionTaskSummaryResults(rnd: rnd, task: task);
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Information($"DoLiabilityCashflowProjectionTasks error - {e.Message}");
            }

            Log.Information("DoLiabilityCashflowProjectionTasks.End SolvencyII");
        }

        // End Refactor

        private async Task CreateBodConfigsAsync()
        {
            Log.Information("CreateBodConfigsAsync.Start");

            try
            {
                if (_context.BodConfigs.Any())
                {
                    Log.Information("CreateBodConfigsAsync.Data already exists for SolvencyII");
                    return;
                }

                var npRegulatoryFund = await _context.WorkspaceRegulatoryFunds
                   .SingleOrDefaultAsync(r => (r.Reference == "Non-Profit Business") && (r.WorkspaceId == _workspaceId));

                var blocksOfBusiness = new List<BodConfig>
                {
                     new BodConfig(workspaceId: _workspaceId,
                        processType: SiiProcessType.Assets,
                        reference: "Asset Manager 1",
                        regulatoryFundId: npRegulatoryFund.Id,
                        modifier: SystemUsers.Seed),

                    new BodConfig(workspaceId: _workspaceId,
                        processType: SiiProcessType.Assets,
                        reference: "Asset Manager 2",
                        regulatoryFundId: npRegulatoryFund.Id,
                        modifier: SystemUsers.Seed)
                };

                await _context.BodConfigs.AddRangeAsync(blocksOfBusiness);

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateBodConfigsAsync");
            }

            Log.Information("CreateBodConfigsAsync.End");
        }

        private async Task CreateWorkflowConfigsAsync()
        {
            Log.Information("CreateWorkflowConfigsAsync.Start");

            try
            {
                if (_context.WorkflowConfigs.Any())
                {
                    Log.Information("CreateWorkflowConfigsAsync.Data   already exists");
                    return;
                }

                var reportingCycleConfigurations = new List<WorkflowConfig>();

                var siiWorkflowConfiguration1 = new WorkflowConfig(workspaceId: _workspaceId,
                    reference: "Data Import Workflow",
                    runCalculateLifeLiabilities: true,
                    runAssetStressTests: true,
                    runCalculateCapital: true,
                    runCalculateRiskMargin: true,
                    runORSA: false,
                    qrtTemplates: QRTTemplatesType.Quarterly,
                    modifier: SystemUsers.Seed);

                siiWorkflowConfiguration1.AddAssetsConfig(new WorkflowAssetsConfig(
                  workspaceId: _workspaceId,
                  includeInterestRateUpStressTest: true,
                  includeInterestRateDownStressTest: false,
                  includeEquityStressTest: false,
                  includePropertyStressTest: false,
                  includeSpreadStressTest: false,
                  includeCurrencyStressTest: false,
                  includeConcentrationStressTest: false,
                  modifier: SystemUsers.Seed
                  ));

                reportingCycleConfigurations.Add(siiWorkflowConfiguration1);

                var siiWorkflowConfiguration2 = new WorkflowConfig(workspaceId: _workspaceId,
                     reference: "Data Mapping Workflow",
                     runCalculateLifeLiabilities: true,
                     runAssetStressTests: false,
                     runCalculateCapital: false,
                     runCalculateRiskMargin: false,
                     runORSA: false,
                     qrtTemplates: QRTTemplatesType.Annual,
                     modifier: SystemUsers.Seed);

                siiWorkflowConfiguration2.AddAssetsConfig(new WorkflowAssetsConfig(
                    workspaceId: _workspaceId,
                    includeInterestRateUpStressTest: false,
                    includeInterestRateDownStressTest: false,
                    includeEquityStressTest: false,
                    includePropertyStressTest: false,
                    includeSpreadStressTest: false,
                    includeCurrencyStressTest: false,
                    includeConcentrationStressTest: false,
                    modifier: SystemUsers.Seed
                    ));

                reportingCycleConfigurations.Add(siiWorkflowConfiguration2);

                var siiWorkflowConfiguration3 = new WorkflowConfig(workspaceId: _workspaceId,
                     reference: "Data Entry Workflow",
                     runCalculateLifeLiabilities: true,
                     runAssetStressTests: false,
                     runCalculateCapital: false,
                     runCalculateRiskMargin: false,
                     runORSA: false,
                     qrtTemplates: QRTTemplatesType.Quarterly,
                     modifier: SystemUsers.Seed);

                siiWorkflowConfiguration3.AddAssetsConfig(new WorkflowAssetsConfig(
                   workspaceId: _workspaceId,
                   includeInterestRateUpStressTest: true,
                   includeInterestRateDownStressTest: false,
                   includeEquityStressTest: true,
                   includePropertyStressTest: false,
                   includeSpreadStressTest: false,
                   includeCurrencyStressTest: false,
                   includeConcentrationStressTest: false,
                   modifier: SystemUsers.Seed
                   ));

                reportingCycleConfigurations.Add(siiWorkflowConfiguration3);

                if (_seedType == SeedType.IntegrationTests)
                {
                    reportingCycleConfigurations.Add(new WorkflowConfig(workspaceId: _workspaceId, reference: "WF Test1", runAssetStressTests: true,
                        runCalculateLifeLiabilities: true, runCalculateCapital: true, runCalculateRiskMargin: true, runORSA: true,
                        qrtTemplates: QRTTemplatesType.Quarterly, modifier: SystemUsers.Seed));
                    reportingCycleConfigurations.Add(new WorkflowConfig(workspaceId: _workspaceId, reference: "WF Test2", runAssetStressTests: true,
                        runCalculateLifeLiabilities: true, runCalculateCapital: true, runCalculateRiskMargin: true, runORSA: true,
                        qrtTemplates: QRTTemplatesType.Quarterly, modifier: SystemUsers.Seed));
                    reportingCycleConfigurations.Add(new WorkflowConfig(workspaceId: _workspaceId, reference: "WF Test3", runAssetStressTests: true,
                        runCalculateLifeLiabilities: true, runCalculateCapital: true, runCalculateRiskMargin: true, runORSA: true,
                        qrtTemplates: QRTTemplatesType.Quarterly, modifier: SystemUsers.Seed));
                }

                _context.WorkflowConfigs.AddRange(reportingCycleConfigurations);

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateWorkflowConfigsAsync");
            }

            Log.Information("CreateWorkflowConfigsAsync.End");
        }

        private AssetsBodConfig CreateAssetsBodConfig(Random rnd,
           BodConfig blockOfBusiness,
           AssetsDataSourceType dataSource)
        {
            var rcBlockOfDataConfiguration = new AssetsBodConfig(workspaceId: _workspaceId,
                     belBodConfigId: blockOfBusiness.Id,
                     dataSource: dataSource,
                     modifier: SystemUsers.Seed);

            return rcBlockOfDataConfiguration;
        }

        private async Task CreateAssetsBodConfigsAsync(Random rnd)
        {
            Log.Information("CreateAssetsBodConfigsAsync.Start");

            try
            {
                if (_context.AssetsBodConfigs.Any())
                {
                    Log.Information("CreateAssetsBodConfigsAsync.Data");
                    return;
                }

                var assetsBlocksOfBusiness = await _context.BodConfigs
                    .Where(b => (b.WorkspaceId == _workspaceId) && (b.ProcessType == SiiProcessType.Assets))
                    .ToListAsync();

                var dataImportRCConfiguration = await _context.WorkflowConfigs
                    .Include(r => r.AssetsConfig)
                    .SingleOrDefaultAsync(r => (r.Reference == "Data Import Workflow") && (r.WorkspaceId == _workspaceId));

                var assetManager1Bod = assetsBlocksOfBusiness.SingleOrDefault(b => b.Reference == "Asset Manager 1");

                dataImportRCConfiguration.AssetsConfig.AddBod(CreateAssetsBodConfig(rnd: rnd,
                    blockOfBusiness: assetManager1Bod,
                    dataSource: AssetsDataSourceType.DataImport));

                var dataMappingRCConfiguration = await _context.WorkflowConfigs
                      .Include(r => r.AssetsConfig)
                      .SingleOrDefaultAsync(r => (r.Reference == "Data Mapping Workflow") && (r.WorkspaceId == _workspaceId));

                var assetManager2Bod = assetsBlocksOfBusiness.SingleOrDefault(b => b.Reference == "Asset Manager 2");

                dataMappingRCConfiguration.AssetsConfig.AddBod(CreateAssetsBodConfig(rnd: rnd,
                    blockOfBusiness: assetManager2Bod,
                    dataSource: AssetsDataSourceType.DataImport));

                var dataEntryRCConfiguration = await _context.WorkflowConfigs
                    .Include(r => r.AssetsConfig)
                    .SingleOrDefaultAsync(r => (r.Reference == "Data Entry Workflow") && (r.WorkspaceId == _workspaceId));

                dataEntryRCConfiguration.AssetsConfig.AddBod(CreateAssetsBodConfig(rnd: rnd,
                    blockOfBusiness: assetManager1Bod,
                    dataSource: AssetsDataSourceType.DataEntry));

                if (_seedType == SeedType.IntegrationTests)
                {
                    dataImportRCConfiguration.AssetsConfig.AddBod(CreateAssetsBodConfig(rnd: rnd,
                        blockOfBusiness: assetManager2Bod,
                        dataSource: AssetsDataSourceType.DataImport));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateAssetsBodConfigsAsync");
            }

            Log.Information("CreateAssetsBodConfigsAsync.End");
        }

        private async Task CreateAssetsAssumptionInstancesAsync(Random rnd)
        {
            Log.Information("CreateAssetsAssumptionInstancesAsync.Start");

            try
            {
                if (_context.AssetsAssumptionInstances.Any())
                {
                    Log.Information("CreateAssetsAssumptionInstancesAsync.Data already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles
                    .Where(r => r.WorkspaceId == _workspaceId)
                    .ToListAsync();

                foreach (var reportingCycle in reportingCycles)
                {
                    reportingCycle.AddAssetsAssumptionInstance(new AssetsAssumptionInstance(workspaceId: _workspaceId, assumptionIdentifier: AssetsAssumptionIdentifier.EquityIndexCurrent, value: "1075.25", modifier: SystemUsers.Seed));
                    reportingCycle.AddAssetsAssumptionInstance(new AssetsAssumptionInstance(workspaceId: _workspaceId, assumptionIdentifier: AssetsAssumptionIdentifier.EquityIndexWeightedAverage, value: "975.25", modifier: SystemUsers.Seed));
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateAssetsAssumptionInstancesAsync");
            }

            Log.Information("CreateAssetsAssumptionInstancesAsync.End");
        }

        private decimal? SetCashflow(Random rnd, int year, int startYear, int lastYear, decimal startingValue, double annualDecrement, decimal? priorYearCashflow)
        {
            return year > lastYear
                ? null
                : year == startYear
                    ? (decimal?)Math.Round((decimal)rnd.NextDouble() * startingValue, 2)
                    : (decimal?)Math.Round((decimal)priorYearCashflow * (decimal)Math.Max(rnd.NextDouble(), annualDecrement), 2);
        }

        private decimal GetCashflowValue(decimal? cashflow)
        {
            return cashflow ?? 0;
        }

        private decimal? GetMortalityRate(Random rnd, GenderType gender, int age)
        {
            decimal? mortalityRate = null;

            switch (gender)
            {
                case GenderType.Male:
                    if (_maleMortalityTable == null)
                    {
                        _maleMortalityTable = GenerateMortalityTable(rnd, "Male");
                    }
                    return _maleMortalityTable.GetAnnualRate(age);

                case GenderType.Female:
                    if (_femaleMortalityTable == null)
                    {
                        _femaleMortalityTable = GenerateMortalityTable(rnd, "Female");
                    }
                    return _femaleMortalityTable.GetAnnualRate(age);
            }

            return mortalityRate;
        }

        private AssetsInputDataSet CreateAssetsInputDataSet(Random rnd, ReportingCycle siiRcInstance, string reference)
        {
            var siiRcBelAssetsBodInputDataSet = new AssetsInputDataSet(workspaceId: _workspaceId,
                    modifier: SystemUsers.Seed);

            //Property
            for (int i = 1; i <= _numberOfAssetsDataPoints; i++)
            {
                siiRcBelAssetsBodInputDataSet.AddItem(new AssetsInputDataSetItem(workspaceId: _workspaceId,
                  reference: $"PROPERTY{rnd.Next(10000, 999999)}",
                  cicCode: i % 2 == 0 ? "GB95" : "GB45",
                  participating: false,
                  marketValue: (decimal)Math.Round(rnd.NextDouble() * 10000, 2),
                  modifier: SystemUsers.Seed));
            };

            //Equity
            for (int i = 1; i <= _numberOfAssetsDataPoints; i++)
            {
                siiRcBelAssetsBodInputDataSet.AddItem(new AssetsInputDataSetItem(workspaceId: _workspaceId,
                  reference: $"EQUITY{rnd.Next(10000, 999999)}",
                  cicCode: i % 2 == 0 ? "GB39" : "XX43",
                  participating: i % 2 == 0,
                  marketValue: (decimal)Math.Round(rnd.NextDouble() * 10000, 2),
                  modifier: SystemUsers.Seed));
            };

            return siiRcBelAssetsBodInputDataSet;
        }

        private async Task<int> CreateAssetsInputDataSetImportFileId(AssetsInputDataSet inputDataSet)
        {
            var storageFileId = Guid.NewGuid().ToString();
            var document = new Document(workspaceId: _workspaceId, name: inputDataSet.Reference,
                fileStorageId: storageFileId,
                type: DocumentType.Text,
                serviceType: ServiceType.SolvencyIIAnalytics,
                modifier: SystemUsers.Seed);

            await _context.WorkspaceDocuments.AddAsync(document);
            await _context.BulkSaveChangesAsync();

            string header = "Instrument ID,CIC Code,Participating,Market Value,Include";

            var csv = new StringBuilder(header);

            foreach (var item in inputDataSet.Items)
            {
                var rowContents = new StringBuilder();
                rowContents.Append(item.Reference); //Instrument Id
                rowContents.Append($",{item.CICCode}"); // CICcode
                rowContents.Append($",{item.Participating}"); // Participating
                rowContents.Append($",{item.MarketValue.ToString("F2", CultureInfo.InvariantCulture)}");
                rowContents.Append($",{item.IncludeInStresses}");

                csv.Append($"\n{rowContents}");
            }

            await _storage.UploadAsync(ServiceType.SolvencyIIAnalytics.GetValue(), storageFileId, csv.ToString());

            return document.Id;
        }

        private async Task CreateAssetsBodInstancesAsync(Random rnd)
        {
            Log.Information("CreateAssetsBodInstancesAsync.Start SolvencyII");

            try
            {
                if (_context.AssetsBodInstances.Any())
                {
                    Log.Information("CreateAssetsBodInstancesAsync.Data  SII already exists");
                    return;
                }

                var reportingCycles = await _context.SIIReportingCycles
                    .Include(r => r.WorkflowConfig)
                        .ThenInclude(r => r.AssetsConfig)
                            .ThenInclude(r => r.BodConfigs)
                                .ThenInclude(r => r.BodConfig)
                    .Where(r => r.WorkspaceId == _workspaceId)
                    .ToListAsync();

                int count = 0;

                foreach (var reportingCycle in reportingCycles)
                {
                    foreach (var assetsBodConfig in reportingCycle.WorkflowConfig.AssetsConfig.BodConfigs)
                    {
                        count++;

                        int? importedFileDocumentId = null;

                        var reference = $"{reportingCycle.Reference} - {assetsBodConfig.BodConfig.Reference}";

                        var inputDataSet = new AssetsInputDataSet(workspaceId: _workspaceId, modifier: SystemUsers.Seed);
                        if ((assetsBodConfig.DataSource == AssetsDataSourceType.DataImport)
                            || (_seedType == SeedType.IntegrationTests)) // so that have items in database
                        {
                            inputDataSet = CreateAssetsInputDataSet(rnd: rnd, siiRcInstance: reportingCycle, reference: reference);
                        }

                        if (assetsBodConfig.DataSource == AssetsDataSourceType.DataImport)
                        {
                            importedFileDocumentId = await CreateAssetsInputDataSetImportFileId(inputDataSet: inputDataSet);
                        }

                        var siiRcAssetsBodInstance = new AssetsBodInstance(workspaceId: _workspaceId,
                                assetsBodConfigId: assetsBodConfig.Id,
                                importedFileDocumentId: importedFileDocumentId,
                                customSchemaDataSetId: null,
                                modifier: SystemUsers.Seed);

                        reportingCycle.AddAssetsBodInstance(siiRcAssetsBodInstance);
                        await _context.BulkSaveChangesAsync();

                        if (_seedType != SeedType.IntegrationTests || count <= 2)
                        {
                            siiRcAssetsBodInstance.AddInputDataSet(inputDataSet);

                            await _context.BulkSaveChangesAsync();
                        }

                        await _context.BulkSaveChangesAsync();
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateAssetsBodInstancesAsync SolvencyII");
            }

            Log.Information("CreateAssetsBodInstancesAsync.End SolvencyII");
        }

        private IList<AssetsTaskInstance> CreateAssetsTaskInstances(Random rnd, ReportingCycle siiRcInstance, AssetsBodInstance siiRcAssetsBodInstance)
        {
            IList<AssetsTaskInstance> taskInstances = new List<AssetsTaskInstance>();

            AssetsTaskType dataSourceTaskType;
            switch (siiRcAssetsBodInstance.AssetsBodConfig.DataSource)
            {
                case AssetsDataSourceType.DataEntry:
                    dataSourceTaskType = AssetsTaskType.DataEntry;
                    break;

                default:
                    dataSourceTaskType = AssetsTaskType.DataImport;
                    break;
            }

            taskInstances.Add(new AssetsTaskInstance(workspaceId: _workspaceId, type: dataSourceTaskType, status: CreateTaskStatus(rnd, siiRcInstance),
                modifier: SystemUsers.Seed));
            taskInstances.Add(new AssetsTaskInstance(workspaceId: _workspaceId, type: AssetsTaskType.MarketValueStress,
                status: CreateTaskStatus(rnd, siiRcInstance),
                modifier: SystemUsers.Seed));

            return taskInstances;
        }

        private async Task CreateAssetsTaskInstancesAsync(Random rnd)
        {
            Log.Information("CreateAssetsTaskInstancesAsync.Start SolvencyII");

            try
            {
                if (_context.AssetsTaskInstances.Any())
                {
                    Log.Information("CreateAssetsTaskInstancesAsync.Data  SII already exists");
                    return;
                }

                var siiRcInstances = await _context.SIIReportingCycles
                    .Include(r => r.AssetsBodInstances)
                        .ThenInclude(r => r.AssetsBodConfig)
                    .Where(r => r.WorkspaceId == _workspaceId)
                    .ToListAsync();

                foreach (var siiRcInstance in siiRcInstances)
                {
                    foreach (var siiRcAssetsBodInstance in siiRcInstance.AssetsBodInstances)
                    {
                        siiRcAssetsBodInstance.AddTasks(CreateAssetsTaskInstances(rnd: rnd,
                            siiRcInstance: siiRcInstance,
                            siiRcAssetsBodInstance: siiRcAssetsBodInstance));
                    }
                }

                await _context.BulkSaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Error(e, "CreateAssetsTaskInstancesAsync SolvencyII");
            }

            Log.Information("CreateAssetsTaskInstancesAsync.End SolvencyII");
        }

        private async Task CreateAssetsTaskResults(Random rnd, int taskId, AssetsMarketValueStressBasisType basis)
        {
            var task = await _context.AssetsTaskInstances
                .Include(t => t.AssetsBodInstance)
                    .ThenInclude(t => t.InputDataSet)
                        .ThenInclude(t => t.Items)
                .SingleOrDefaultAsync(t => t.Id == taskId);

            AssetsSelectedResultsSet selectedResultsSet = new AssetsSelectedResultsSet(workspaceId: _workspaceId, basis: basis, modifier: "seed");
            task.AddSelectedResultsSet(selectedResultsSet);

            foreach (var inputDataItem in task.AssetsBodInstance.InputDataSet.Items)
            {
                selectedResultsSet.AddItem(new AssetsSelectedResultsSetItem(inputDataSetItem: inputDataItem,
                    equityType1MarketValue: inputDataItem.GetInstrumentType() == AssetInstrumentType.Equity ? (decimal?)Math.Round(rnd.NextDouble() * 10000, 2) : null,
                    equityType2MarketValue: inputDataItem.GetInstrumentType() == AssetInstrumentType.Equity ? (decimal?)Math.Round(rnd.NextDouble() * 10000, 2) : null,
                    marketValue: (decimal)Math.Round(rnd.NextDouble() * 10000, 2),
                    modifier: "seed"));
            }

            foreach (var inputDataItem in task.AssetsBodInstance.InputDataSet.Items)
            {
                AssetsDetailedCashflowsSet detailedCashflowSet = new AssetsDetailedCashflowsSet(workspaceId: _workspaceId, basis: basis, inputDataSetItemId: inputDataItem.Id, modifier: "seed");

                int numPeriods = _seedType == SeedType.IntegrationTests ? 2 : 0;

                for (int t = 0; t <= numPeriods; t++)
                {
                    detailedCashflowSet.AddItem(new AssetsDetailedCashflowsSetItem(workspaceId: _workspaceId,
                        reference: t.ToString(),
                        date: DateTime.UtcNow,
                        equityType1MarketValue: inputDataItem.GetInstrumentType() == AssetInstrumentType.Equity ? (decimal?)Math.Round(rnd.NextDouble() * 10000, 2) : null,
                        equityType2MarketValue: inputDataItem.GetInstrumentType() == AssetInstrumentType.Equity ? (decimal?)Math.Round(rnd.NextDouble() * 10000, 2) : null,
                        marketValue: (decimal)Math.Round(rnd.NextDouble() * 10000, 2),
                        modifier: "seed"));
                }

                task.AddDetailedCashflowSet(detailedCashflowSet);
            }

            foreach (var assetInstrumentType in new List<AssetInstrumentType>() { AssetInstrumentType.Property, AssetInstrumentType.Equity })
            {
                AssetsCashflowsSet cashflowSet = new AssetsCashflowsSet(workspaceId: _workspaceId, basis: basis, assetInstrumentType: assetInstrumentType, modifier: "seed");

                int numPeriods = _seedType == SeedType.IntegrationTests ? 2 : 0;

                for (int t = 0; t <= numPeriods; t++)
                {
                    cashflowSet.AddItem(new AssetsCashflowsSetItem(workspaceId: _workspaceId,
                        reference: t.ToString(),
                        marketValue: (decimal)Math.Round(rnd.NextDouble() * 10000, 2),
                        modifier: "seed"));
                }

                task.AddCashflowSet(cashflowSet);
            }

            await _context.BulkSaveChangesAsync();
        }

        private async Task DoAssetsStressCalculations(Random rnd)
        {
            var siiRcInstances = await _context.SIIReportingCycles
                .AsNoTracking()
                .Include(r => r.WorkflowConfig)
                .ThenInclude(r => r.AssetsConfig)
                .Include(r => r.AssetsBodInstances)
                    .ThenInclude(r => r.Tasks)
                .Include(r => r.AssetsBodInstances)
                    .ThenInclude(r => r.InputDataSet)
                .Where(r => r.WorkspaceId == _workspaceId)
                .ToListAsync();

            foreach (var siiRcInstance in siiRcInstances)
            {
                foreach (var siiRcAssetsBodInstance in siiRcInstance.AssetsBodInstances)
                {
                    int count = 0;
                    foreach (var task in siiRcAssetsBodInstance.Tasks.Where(t => t.Type == AssetsTaskType.MarketValueStress).ToList())
                    {
                        foreach (var basis in siiRcInstance.WorkflowConfig.AssetsConfig.GetBases())
                        {
                            count++;
                            if (_seedType == SeedType.Full || basis == AssetsMarketValueStressBasisType.Base || count <= 2)
                            {
                                if (task.AssetsBodInstance.InputDataSet != null)
                                {
                                    Log.Information($"Starting asset market value task {basis}");

                                    if (_seedType == SeedType.IntegrationTests || !_performSIIAssetsCalculations)
                                    {
                                        await CreateAssetsTaskResults(rnd: rnd, taskId: task.Id, basis: basis);
                                    }
                                    else
                                    {
                                        await _assetsMarketValueStressTaskInstanceExecutor.ExecuteAssetsMarketValueStressTask(task.Id, basis);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}