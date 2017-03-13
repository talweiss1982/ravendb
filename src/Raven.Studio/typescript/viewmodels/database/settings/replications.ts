import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/database/replication/replicationsSetup");
import replicationConfig = require("models/database/replication/replicationConfig")
import replicationDestination = require("models/database/replication/replicationDestination");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/database/replication/getReplicationsCommand");
import updateServerPrefixHiLoCommand = require("commands/database/documents/updateServerPrefixHiLoCommand");
import saveReplicationDocumentCommand = require("commands/database/replication/saveReplicationDocumentCommand");
import saveAutomaticConflictResolutionDocumentCommand = require("commands/database/replication/saveAutomaticConflictResolutionDocumentCommand");
import getServerPrefixForHiLoCommand = require("commands/database/documents/getServerPrefixForHiLoCommand");
import replicateAllIndexesCommand = require("commands/database/replication/replicateAllIndexesCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import replicateAllTransformersCommand = require("commands/database/replication/replicateAllTransformersCommand");
import replicateIndexesCommand = require("commands/database/replication/replicateIndexesCommand");
import replicateTransformersCommand = require("commands/database/replication/replicateTransformersCommand");
import getEffectiveConflictResolutionCommand = require("commands/database/globalConfig/getEffectiveConflictResolutionCommand");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import enableReplicationCommand = require("commands/database/replication/enableReplicationCommand");
import resolveAllConflictsCommand = require("commands/database/replication/resolveAllConflictsCommand");
import eventsCollector = require("common/eventsCollector");

class replications extends viewModelBase {

    replicationEnabled = ko.observable<boolean>(true);

    prefixForHilo = ko.observable<string>("");
    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ Source: null, Destinations: [], ClientConfiguration: null, DocumentConflictResolution: null, Id: null }));

    serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([]);
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    isServerPrefixForHiLoSaveEnabled: KnockoutComputed<boolean>;
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    skipIndexReplicationForAllDestinationsStatus = ko.observable<string>();
    skipIndexReplicationForAll = ko.observable<boolean>();

    showRequestTimeoutRow: KnockoutComputed<boolean>;

    private skipIndexReplicationForAllSubscription: KnockoutSubscription;

    private refereshSkipIndexReplicationForAllDestinations() {
        if (this.skipIndexReplicationForAllSubscription != null)
            this.skipIndexReplicationForAllSubscription.dispose();

        var newStatus = this.getIndexReplicationStatusForAllDestinations();
        this.skipIndexReplicationForAll(newStatus === 'all');

        this.skipIndexReplicationForAllSubscription = this.skipIndexReplicationForAll.subscribe(newValue => this.toggleIndexReplication(newValue));
    }

    private getIndexReplicationStatusForAllDestinations(): string {
        var nonEtlDestinations = this.replicationsSetup().destinations().filter(x => !x.enableReplicateOnlyFromCollections());
        var nonEtlWithSkipedIndexReplicationCount = nonEtlDestinations.filter(x => x.skipIndexReplication()).length;

        if (nonEtlWithSkipedIndexReplicationCount === 0)
            return 'none';

        if (nonEtlWithSkipedIndexReplicationCount === nonEtlDestinations.length)
            return 'all';

        return 'mixed';
    }

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.showRequestTimeoutRow = ko.computed(() => this.replicationsSetup().showRequestTimeSlaThreshold());
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            //TODO: we don't have active bundles in v4.0, let assume it is enabled
            this.replicationEnabled(true);
            $.when(this.fetchServerPrefixForHiLoCommand(db), this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));

        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("7K1KES");

        this.serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([this.prefixForHilo]);
        this.isServerPrefixForHiLoSaveEnabled = ko.computed(() => this.serverPrefixForHiLoDirtyFlag().isDirty());
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => this.replicationConfigDirtyFlag().isDirty());

        var replicationSetupDirtyFlagItems = [this.replicationsSetup, this.replicationsSetup().destinations(), this.skipIndexReplicationForAll, this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour];

        this.replicationsSetupDirtyFlag = new ko.DirtyFlag(replicationSetupDirtyFlagItems);

        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        var combinedFlag = ko.computed(() => {
            var rc = this.replicationConfigDirtyFlag().isDirty();
            var rs = this.replicationsSetupDirtyFlag().isDirty();
            var sp = this.serverPrefixForHiLoDirtyFlag().isDirty();
            return rc || rs || sp;
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    attached() {
        super.attached();
        $.each(this.replicationsSetup().destinations(), this.addScriptHelpPopover);
    }

    private fetchServerPrefixForHiLoCommand(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getServerPrefixForHiLoCommand(db)
            .execute()
            .done((result) => this.prefixForHilo(result))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchAutomaticConflictResolution(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getEffectiveConflictResolutionCommand(db)
            .execute()
            .done((repConfig: configurationDocumentDto<replicationConfigDto>) => {
                this.replicationConfig(new replicationConfig(repConfig.MergedDocument));
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchReplications(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        ko.postbox.subscribe('skip-index-replication', () => this.refereshSkipIndexReplicationForAllDestinations());

        new getReplicationsCommand(db)
            .execute()
            .done((repSetup: Raven.Client.Documents.Replication.ReplicationDocument<Raven.Client.Documents.Replication.ReplicationDestination>) => {
                if (repSetup) {
                    this.replicationsSetup(new replicationsSetup(repSetup));    
                }

                var status = this.getIndexReplicationStatusForAllDestinations();
                if (status === 'all')
                    this.skipIndexReplicationForAll(true);
                else
                    this.skipIndexReplicationForAll(false);

                this.skipIndexReplicationForAllSubscription = this.skipIndexReplicationForAll.subscribe(newValue => this.toggleIndexReplication(newValue));
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    addScriptHelpPopover() {
        $(".scriptPopover").popover({
            html: true,
            trigger: 'hover',
            container: ".form-horizontal",
            content:
            '<p>Return <code>null</code> in transform script to skip document from replication. </p>' +
            '<p>Example: </p>' +
            '<pre><span class="code-keyword">if</span> (<span class="code-keyword">this</span>.Region !== <span class="code-string">"Europe"</span>) { <br />   <span class="code-keyword">return null</span>; <br />}<br/><span class="code-keyword">this</span>.Currency = <span class="code-string">"EUR"</span>; </pre>'
        });
    }

    public onTransformCollectionClick(destination: replicationDestination, collectionName: string) {
        var collections = destination.specifiedCollections();
        var item = collections.find(c => c.collection() === collectionName);

        if (typeof (item.script()) === "undefined") {
            item.script("");
        } else {
            item.script(undefined);
        }

        destination.specifiedCollections.notifySubscribers();
    }

    toggleSkipIndexReplicationForAll() {
        eventsCollector.default.reportEvent("replications", "toggle-skip-for-all");

        this.skipIndexReplicationForAll.toggle();
    }

    createNewDestination() {
        eventsCollector.default.reportEvent("replications", "create");

        var db = this.activeDatabase();
        this.replicationsSetup().destinations.unshift(replicationDestination.empty(db.name));
        this.refereshSkipIndexReplicationForAllDestinations();
        this.addScriptHelpPopover();
    }

    removeDestination(repl: replicationDestination) {
        eventsCollector.default.reportEvent("replications", "toggle-skip-for-all");

        this.replicationsSetup().destinations.remove(repl);
    }

    saveChanges() {
        eventsCollector.default.reportEvent("replications", "save");

        if (this.isConfigSaveEnabled())
            this.saveAutomaticConflictResolutionSettings();
        if (this.isSetupSaveEnabled()) {
            if (this.replicationsSetup().source()) {
                this.saveReplicationSetup();
            } else {
                var db = this.activeDatabase();
                if (db) {
                    new getDatabaseStatsCommand(db)
                        .execute()
                        .done(result=> {
                            this.prepareAndSaveReplicationSetup(result.DatabaseId);
                        });
                }
            }
        }
    }

    private prepareAndSaveReplicationSetup(source: string) {
        this.replicationsSetup().source(source);
        this.saveReplicationSetup();
    }

    private saveReplicationSetup() {
        var db = this.activeDatabase();
        if (db) {
            new saveReplicationDocumentCommand(this.replicationsSetup().toDto(), db)
                .execute()
                .done(() => {
                    this.replicationsSetupDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    toggleIndexReplication(skipReplicationValue: boolean) {
        this.replicationsSetup().destinations().forEach(dest => {
            // since we are on replications page filter toggle to non-etl destinations only
            if (!dest.enableReplicateOnlyFromCollections()) {
                dest.skipIndexReplication(skipReplicationValue);    
            }
        });
    }

    sendReplicateCommand(destination: replicationDestination, parentClass: replications) {
        eventsCollector.default.reportEvent("replications", "send-replicate");

        var db = parentClass.activeDatabase();
        if (db) {
            new replicateIndexesCommand(db, destination).execute();
            new replicateTransformersCommand(db, destination).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }
    }

    sendReplicateAllCommand() {
        eventsCollector.default.reportEvent("replications", "send-replicate-all");

        var db = this.activeDatabase();
        if (db) {
            new replicateAllIndexesCommand(db).execute();
            new replicateAllTransformersCommand(db).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }

    }

    sendResolveAllConflictsCommand() {
        eventsCollector.default.reportEvent("replications", "resolve-all");

        var db = this.activeDatabase();
        if (db) {
            new resolveAllConflictsCommand(db).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }
    }

    saveServerPrefixForHiLo() {
        eventsCollector.default.reportEvent("replications", "save-hilo-prefix");

        var db = this.activeDatabase();
        if (db) {
            new updateServerPrefixHiLoCommand(this.prefixForHilo(), db)
                .execute()
                .done(() => {
                    this.serverPrefixForHiLoDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    saveAutomaticConflictResolutionSettings() {
        eventsCollector.default.reportEvent("replications", "save-auto-conflict-resolution");

        var db = this.activeDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocumentCommand(this.replicationConfig().toDto(), db)
                .execute()
                .done(() => {
                    this.replicationConfigDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    enableReplication() {
        eventsCollector.default.reportEvent("replications", "enable-replication");

        new enableReplicationCommand(this.activeDatabase())
            .execute()
            .done((bundles) => {
                var db = this.activeDatabase();
                db.activeBundles(bundles);
                this.replicationEnabled(true);
                this.fetchServerPrefixForHiLoCommand(db);
                this.fetchAutomaticConflictResolution(db);
                this.fetchReplications(db);
            });
    }

}

export = replications; 
