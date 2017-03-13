import viewModelBase = require("viewmodels/viewModelBase");

class globalConfigVersioning extends viewModelBase {
    /* TODO:
    activated = ko.observable<boolean>(false);

    developerLicense = globalConfig.developerLicense;
    canUseGlobalConfigurations = globalConfig.canUseGlobalConfigurations;
    versionings = ko.observableArray<versioningEntry>();
    toRemove: versioningEntry[];
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        if (settingsAccessAuthorizer.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            this.fetchVersioningEntries(null)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseSettings(null) }));
        }
        
        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("1UZ5WL");
        this.toRemove = [];

        this.dirtyFlag = new ko.DirtyFlag([this.versionings]);
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchVersioningEntries(db: database): JQueryPromise<any> {
        var task: JQueryPromise<versioningEntry[]> = new getVersioningsCommand(db, true).execute();

        task.done((versionings: versioningEntry[]) => this.versioningsLoaded(versionings));

        return task;
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        if (deleteConfig) {
            var deleteTask = new saveVersioningCommand(
                null,
                [],
                this.versionings().map((v) => { return v.toDto(true); }).concat(this.toRemove.map((v) => { return v.toDto(true); })),
                true).execute();
            deleteTask.done((saveResult: bulkDocumentDto[]) => {
                this.activated(false);
                this.versionings([]);
                this.versioningsSaved(saveResult);
            });
        } else {
            var saveTask = new saveVersioningCommand(
                null,
                this.versionings().map((v) => { return v.toDto(true); }),
                this.toRemove.map((v) => { return v.toDto(true); }),
                true).execute();
            saveTask.done((saveResult: bulkDocumentDto[]) => {
                this.versioningsSaved(saveResult);
            });
        }
    }

    createNewVersioning() {
        this.versionings.push(new versioningEntry());
    }

    removeVersioning(entry: versioningEntry) {
        if (entry.fromDatabase()) {
            // If this entry is in database schedule the removal
            this.toRemove.push(entry);
        }
        this.versionings.remove(entry);
    }

    versioningsLoaded(data: versioningEntry[]) {
        this.versionings(data);
        this.dirtyFlag().reset();
        this.activated(data.length > 0);
    }

    versioningsSaved(saveResult: bulkDocumentDto[]) {
        for (var i = 0; i < this.versionings().length; i++) {
            this.versionings()[i].__metadata.etag = saveResult[i].Etag;
        }

        // After save the collection names are not editable
        this.versionings().forEach((v: versioningEntry) => {
            v.fromDatabase(true);
        });

        this.dirtyFlag().reset();
        this.toRemove = [];
    }

    activateConfig() {
        this.activated(true);
        this.versionings.push(this.defaultVersioningEntry());
    }

    private defaultVersioningEntry() {
        var entry = new versioningEntry({
            Id: "DefaultConfiguration",
            MaxRevisions: 5,
            Exclude: false,
            ExcludeUnlessExplicit: false,
            PurgeOnDelete: false
        });
        entry.fromDatabase(true);

        return entry;
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for versioning?", "Are you sure?")
            .done(() => {
                this.activated(false);
                this.syncChanges(true);
            });
    }

    makeExcluded(entry: versioningEntry) {
        entry.exclude(true);
    }

    makeIncluded(entry: versioningEntry) {
        entry.exclude(false);
    }*/

    /*
        TODO @gregolsky apply google analytics
    */
}

export = globalConfigVersioning;
