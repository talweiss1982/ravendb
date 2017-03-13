import router = require("plugins/router");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import conflictVersion = require("models/database/replication/conflictVersion");
import getReplicationSourcesCommand = require("commands/database/replication/getReplicationSourcesCommand");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import getSingleTransformerCommand = require("commands/database/transformers/getSingleTransformerCommand");
import conflictsResolveCommand = require("commands/database/replication/conflictsResolveCommand");
import getEffectiveConflictResolutionCommand = require("commands/database/globalConfig/getEffectiveConflictResolutionCommand");
import eventsCollector = require("common/eventsCollector");

import viewModelBase = require("viewmodels/viewModelBase");

class conflicts extends viewModelBase {

    displayName = "conflicts";
    sourcesLookup: dictionary<string> = {};

    private refreshConflictsObservable = ko.observable<number>();
    private conflictsSubscription: KnockoutSubscription;
    //TODO: currentColumns = ko.observable(customColumns.empty());
    hasAnyConflict: KnockoutComputed<boolean>;

    static performedIndexChecks: Array<string> = [];
    static conflictsIndexName = "Raven/ConflictDocuments";
    static conflictsTransformerName = "Raven/ConflictDocumentsTransformer";

    currentConflictsPagedItems = ko.observable<any>(); //TODO: use virtualGrid + type
    selectedDocumentIndices = ko.observableArray<number>();

    serverConflictResolution = ko.observable<string>();

    static gridSelector = "#conflictsGrid";

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.databaseChangesApi();
        //TODO: this.addNotification changesApi.watchAllReplicationConflicts((e) => this.refreshConflictsObservable(new Date().getTime())) 
    }

    attached() {
        super.attached();
        this.conflictsSubscription = this.refreshConflictsObservable.throttle(3000).subscribe((e) => this.fetchConflicts(this.activeDatabase()));
    }

    detached() {
        super.detached();

        if (this.conflictsSubscription != null) {
            this.conflictsSubscription.dispose();
        }
    }

    activate(args: any) {
        super.activate(args);
        this.activeDatabase.subscribe((db: database) => this.databaseChanged(db));

        this.updateHelpLink('5FRRCA');

        this.hasAnyConflict = ko.computed(() => {
            var pagedItems = this.currentConflictsPagedItems();
            if (pagedItems) {
                return pagedItems.totalResultCount() > 0;
            }
            return false;
        });

        /* TODO
        this.currentColumns().columns([
            new customColumnParams({ Header: "Detected At (UTC)", Binding: "conflictDetectedAt", DefaultWidth: 300 }),
            new customColumnParams({ Header: "Versions", Binding: "versions", DefaultWidth: 400, Template: 'versions-template' })
        ]);
        this.currentColumns().customMode(true);*/

        return this.performIndexCheck(this.activeDatabase()).then(() => {
            return this.loadReplicationSources(this.activeDatabase());
        }).done(() => {
            this.fetchAutomaticConflictResolution(this.activeDatabase());
            this.fetchConflicts(this.activeDatabase());
        });
    }

    fetchAutomaticConflictResolution(db: database): JQueryPromise<any> {
        return new getEffectiveConflictResolutionCommand(db)
            .execute()
            .done((repConfig: configurationDocumentDto<replicationConfigDto>) => {
                this.serverConflictResolution(repConfig.MergedDocument.DocumentConflictResolution);
            });
    }

    fetchConflicts(database: database) {
        //TODO: this.currentConflictsPagedItems(this.createPagedList(database));
    }

    loadReplicationSources(db: database): JQueryPromise<dictionary<string>> {
        return new getReplicationSourcesCommand(db)
            .execute()
            .done(results => this.replicationSourcesLoaded(results, db));
    }

    performIndexCheck(db: database): JQueryPromise<any> {

        // first look in cache
        if (_.includes(conflicts.performedIndexChecks, db.name)) {
            return $.Deferred<any>().resolve();
        }

        var performCheckTask = $.Deferred<any>();

        // perform index check against DB
        $.when<any>(new getIndexDefinitionCommand(conflicts.conflictsIndexName, db).execute(),
            new getSingleTransformerCommand(conflicts.conflictsTransformerName, db).execute())
            .done(() => {
                conflicts.performedIndexChecks.push(db.name);
                performCheckTask.resolve();
            })
            .fail( 
            function () {
                conflicts.performedIndexChecks.push(db.name);
            });

        return performCheckTask;
    }

    replicationSourcesLoaded(sources: dictionary<string> , db: database) {
        this.sourcesLookup = sources;
    }

    databaseChanged(db: database) {
        var conflictsUrl = appUrl.forConflicts(db);
        router.navigate(conflictsUrl, false);
        this.performIndexCheck(db).then(() => {
            return this.loadReplicationSources(db);
        }).done(() => {
                this.fetchConflicts(db);
        });
    }

    /* TODO
    private createPagedList(database: database): pagedList {
        var fetcher = (skip: number, take: number) => new getConflictsCommand(database, skip, take).execute();
        return new pagedList(fetcher);
    }*/

    getUrlForConflict(conflictVersion: conflictVersion) {
        return appUrl.forEditDoc(conflictVersion.id, this.activeDatabase());
    }

    getTextForVersion(conflictVersion: conflictVersion) {
        var replicationSource = this.sourcesLookup[conflictVersion.sourceId];
        var text = "";
        if (replicationSource) {
            text = " (" + replicationSource + ")";
        }
        return text;
    }

    getServerUrlForVersion(conflictVersion: conflictVersion) {
        return this.sourcesLookup[conflictVersion.sourceId] || "";
    }

    resolveToLocal() {
        eventsCollector.default.reportEvent("conflicts", "resolve-to-local");
        this.confirmationMessage("Sure?", "You're resolving all conflicts to local.", ["No", "Yes"])
            .done(() => {
                this.performResolve("ResolveToLocal");
            });
    }

    resolveToNewestRemote() {
        eventsCollector.default.reportEvent("conflicts", "resolve-to-newest-remote");
        this.confirmationMessage("Sure?", "You're resolving all conflicts to newest remote.", ["No", "Yes"])
            .done(() => {
            this.performResolve("ResolveToRemote");
        });
    }

    resolveToLatest() {
        eventsCollector.default.reportEvent("conflicts", "resolve-to-latest");
        this.confirmationMessage("Sure?", "You're resolving all conflicts to latest.", ["No", "Yes"])
            .done(() => {
            this.performResolve("ResolveToLatest");
        });
    }
    
    private performResolve(resolution: string) {
        new conflictsResolveCommand(this.activeDatabase(), resolution)
            .execute()
            .done(() => {
                this.fetchConflicts(this.activeDatabase());
            });
    }

}

export = conflicts;
