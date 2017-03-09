import app = require("durandal/app");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import shell = require("viewmodels/shell");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import appUrl = require("common/appUrl");
import resourcesManager = require("common/shell/resourcesManager");
import database = require("models/resources/database");

class watchTrafficConfigDialog extends dialogViewModelBase {
    private resourcesManager = resourcesManager.default;
    public configurationTask = $.Deferred();
    
    watchedResourceMode = ko.observable("SingleResourceView");
    resourceName = ko.observable<string>("");
    lastSearchedwatchedResourceName = ko.observable<string>();
    resourceAutocompletes = ko.observableArray<string>([]);
    maxEntries = ko.observable<number>(1000);
    allDatabasesNames: Array<string>;
    nameCustomValidityError: KnockoutComputed<string>;
    searchResults: KnockoutComputed<Array<string>>;

    constructor() {
        super();
        this.allDatabasesNames = this.resourcesManager.databases().map(x => x.name);

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.allDatabasesNames.filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var newResourceName = this.resourceName();

            var isSingleResourceView = this.watchedResourceMode() === "SingleResourceView";
            if (isSingleResourceView) {
                if (!newResourceName) {
                    errorMessage = "Resource name is required";
                } else {
                    var foundResource = this.allDatabasesNames.find((name: string) => name === newResourceName);
                    if (!foundResource) {
                        errorMessage = "Resource name doesn't exist!";
                    }
                }
            }
            
            return errorMessage;
        });
    }

    bindingComplete() {
        document.getElementById("watchedResource").focus();
    }
    
    enterKeyPressed() {
        return true;
    }

    fetchResourcesAutocompletes(search: string) {
        if (this.resourceName() === search) {
            if (this.resourceAutocompletes.length === 1 && this.resourceName() === this.resourceAutocompletes()[0]) {
                this.resourceAutocompletes.removeAll();
                return;
            }
            this.resourceAutocompletes(this.allDatabasesNames.filter((name: string) => name.toLowerCase().indexOf(search.toLowerCase()) === 0));
        }
    }
    
    cancel() {
        dialog.close(this);
    }

    deactivate() {
        this.configurationTask.reject();
    }

    confirmConfig() {
        var tracedResource: database;
        if (this.watchedResourceMode() === "SingleResourceView")
            tracedResource = this.resourcesManager.databases().find((db: database) => db.name === this.resourceName());

        var resourcePath = appUrl.forDatabaseQuery(tracedResource);
        
        var getTokenTask = new getSingleAuthTokenCommand(tracedResource, this.watchedResourceMode() === "AdminView").execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                this.configurationTask.resolve({
                    Resource: tracedResource,
                    ResourceName: tracedResource.name,
                    ResourcePath: resourcePath,
                    MaxEntries: this.maxEntries(),
                    WatchedResourceMode: this.watchedResourceMode(),
                    SingleAuthToken:tokenObject
                });
                dialog.close(this);
            })
            .fail((e) => {
                var response = JSON.parse(e.responseText);
                var msg = e.statusText;
                if ("Error" in response) {
                    msg += ": " + response.Error;
                } else if ("Reason" in response) {
                    msg += ": " + response.Reason;
                }
                app.showBootstrapMessage(msg, "Error");
        });
    }
    
    generateBindingInputId(index: number) {
        return "binding-" + index;
    }
}

export = watchTrafficConfigDialog;
