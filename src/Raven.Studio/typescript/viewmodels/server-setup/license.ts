import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");

class license extends setupStep {

    spinners = {
        next: ko.observable<boolean>(false)
    };
    
    licenseUrl = "https://ravendb.net/license/request";

    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && (mode === "Secured" || mode === "LetsEncrypt")) {
            return $.when({ can: true });
        }
        
        return $.when({ redirect: "#welcome" });
    }

    save() {
        if (this.isValid(this.model.license().validationGroup)) {
            const model = this.model;

            this.spinners.next(true); // don't set this back to false
            
            switch (model.mode()) {
                case "LetsEncrypt":
                    router.navigate("#domain");
                    break;
                case "Secured":
                    router.navigate("#nodes");
                    break;
                default:
                    router.navigate("#welcome");
                    break;
            }
        }
    }

}

export = license;
