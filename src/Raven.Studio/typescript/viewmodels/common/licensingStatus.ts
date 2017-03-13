import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
type attributeItem = {
    displayName: string;
    value: string;
};

class licensingStatus extends dialogViewModelBase {

    isHotSpare = false;
    isDevelopmentOnly = false;
    isNonExpiredCommercial = false;
    isExpired = false;
    licenseStatusText: string;
    licenseExpiresAt: string;

    supportStatus: string;
    isCommunitySupport: boolean;
    isProfessionalSupport: boolean;
    isProductionSupport: boolean;
    isPartialSupport: boolean;
    supportExpiresAt: string;

    licensePath: string;

    attrRam: string;
    attrCpus: string;
    attributes: attributeItem[];

    constructor(private licenseStatus: Raven.Server.Commercial.LicenseStatus, supportCoverage: supportCoverageDto, hotSpare: HotSpareDto) {
        super();

        /* TODO
        if (licenseStatus.Attributes.hotSpare === "true") {
            this.isHotSpare = true;
            
            if (hotSpare.ActivationMode === "Activated") {
                this.licenseStatusText = "Hot Spare: Activated";
                this.licenseExpiresAt = moment(hotSpare.ActivationTime).add("days", 4).format("YYYY-MMM-DD");
            } else {
                this.licenseStatusText = "Hot Spare: Not Activated";
                this.licenseExpiresAt = null;
            }
            
        } else {
            this.isDevelopmentOnly = !licenseStatus.IsCommercial;
            this.isNonExpiredCommercial = licenseStatus.IsCommercial && !licenseStatus.Status.contains("Expired");
            this.isExpired = licenseStatus.IsCommercial && licenseStatus.Status.contains("Expired");
            this.licenseStatusText = licenseStatus.Status;
            this.licenseExpiresAt = licenseStatus.Attributes.updatesExpiration;
        }

        this.supportStatus = supportCoverage.Status;
        this.isProfessionalSupport = supportCoverage.Status === 'ProfessionalSupport';
        this.isProductionSupport = supportCoverage.Status === 'ProductionSupport';
        this.isPartialSupport = supportCoverage.Status === 'PartialSupport';
        this.isCommunitySupport = !this.isProfessionalSupport && !this.isProductionSupport && !this.isPartialSupport;

        var rawFormatted = licenseStatus.Attributes.maxRamUtilization === "unlimited" ? "unlimited" : genUtils.formatBytesToSize(parseInt(licenseStatus.Attributes.maxRamUtilization));
        this.attrRam = this.prepareHtmlForAttribute(rawFormatted);
        this.attrCpus = this.prepareHtmlForAttribute(licenseStatus.Attributes.maxParallelism);

        
        this.supportExpiresAt = supportCoverage.EndsAt ? moment(supportCoverage.EndsAt).format("YYYY-MMM-DD") : null;

        this.licensePath = licenseStatus.LicensePath;

        this.attributes = [
            { displayName: "Databases", value: licenseStatus.Attributes.numberOfDatabases },
            { displayName: "Database size", value: licenseStatus.Attributes.maxSizeInMb },
            { displayName: "RavenFS", value: licenseStatus.Attributes.ravenfs },
            { displayName: "Periodic Backup", value: licenseStatus.Attributes.periodicBackup },
            { displayName: "Replication", value: licenseStatus.Attributes.replication },
            { displayName: "Encryption", value: licenseStatus.Attributes.encryption },
            { displayName: "Compression", value: licenseStatus.Attributes.compression },
            { displayName: "FIPS Compliance", value: licenseStatus.Attributes.fips },
            { displayName: "Quotas", value: licenseStatus.Attributes.quotas },
            { displayName: "Global Configuration", value: licenseStatus.Attributes.globalConfigurations },
            { displayName: "Authorization", value: licenseStatus.Attributes.authorization },
            { displayName: "Document Expiration", value: licenseStatus.Attributes.documentExpiration },
            { displayName: "Versioning", value: licenseStatus.Attributes.versioning },
            { displayName: "Cluster", value: licenseStatus.Attributes.clustering },
            { displayName: "Monitoring", value: licenseStatus.Attributes.monitoring },
            { displayName: "Hot Spare", value: licenseStatus.Attributes.hotSpare },
            { displayName: "Allow Windows Clustering", value: licenseStatus.Attributes.allowWindowsClustering }
        ];

        this.attributes.forEach(attr => attr.value = this.prepareHtmlForAttribute(attr.value));*/
    }

    private prepareHtmlForAttribute(input: string) {
        if (input === 'unlimited') {
            return '<i class="fa fa-infinity"></i>';
        } else if (input === 'true') {
            return '<i class="fa fa-check"></i>';
        } else if (input === 'false' || !input) {
            return '<i class="icon-cancel"></i>';
        } 
        return input;
    }

    cancel() {
        dialog.close(this);
    }

    ok() {
        dialog.close(this);
    }

}

export = licensingStatus;
