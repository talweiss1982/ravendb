import viewModelBase = require("viewmodels/viewModelBase");
import licenseCheckConnectivityCommand = require("commands/auth/licenseCheckConnectivityCommand");
import forceLicenseUpdate = require("commands/auth/forceLicenseUpdate");
import licensingStatus = require("viewmodels/common/licensingStatus");
import app = require("durandal/app");
import license = require("models/auth/license");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import eventsCollector = require("common/eventsCollector");

class licenseInformation extends viewModelBase {

    settingsAccess = new settingsAccessAuthorizer();
    connectivityStatus = ko.observable<string>("pending");
    forceButtonEnabled = ko.pureComputed(() => this.connectivityStatus() === "success");
    forceInProgress = ko.observable<boolean>(false);

    attached() {
        super.attached();

        if (settingsAccessAuthorizer.canReadOrWrite()) {
            this.checkConnectivity()
                .done((result) => {
                    this.connectivityStatus(result ? "success" : "failed");
                })
                .fail(() => this.connectivityStatus("failed"));
        }
    }

    fetchLicenseStatus() {
        /* TODO
        return new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => {
            if (result.Status.includes("AGPL")) {
                result.Status = "Development Only";
            }
            license.licenseStatus(result);
        });*/
    }

    fetchSupportCoverage() {
        return new getSupportCoverageCommand()
            .execute()
            .done((result: supportCoverageDto) => {
                license.supportCoverage(result);
            });
    }


    forceUpdate() {
        eventsCollector.default.reportEvent("license-information", "force-update");

        this.forceInProgress(true);
        new forceLicenseUpdate().execute()
            .always(() => {
                $.when<any>(this.fetchLicenseStatus(), this.fetchSupportCoverage())
                    .always(() => {
                        this.forceInProgress(false);
                        this.showLicenseDialog();
                    });
            });
    }

    private showLicenseDialog() {
        var dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage(), license.hotSpare());
        app.showBootstrapDialog(dialog);
    }

    checkConnectivity(): JQueryPromise<boolean> {
        return new licenseCheckConnectivityCommand().execute();
    }
}

export =licenseInformation;
