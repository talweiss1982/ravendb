import viewModelBase = require("viewmodels/viewModelBase");

class etl extends viewModelBase {
    /*TODO

    replicationEnabled = ko.observable<boolean>(false);

    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));
    collections = ko.observableArray<collection>();

    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    isServerPrefixForHiLoSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    skipIndexReplicationForAllDestinationsStatus = ko.observable<string>();
    skipIndexReplicationForAll = ko.observable<boolean>();

    private skipIndexReplicationForAllSubscription: KnockoutSubscription;

    private refereshSkipIndexReplicationForAllDestinations() {
        if (this.skipIndexReplicationForAllSubscription != null)
            this.skipIndexReplicationForAllSubscription.dispose();

        var newStatus = this.getIndexReplicationStatusForAllDestinations();
        this.skipIndexReplicationForAll(newStatus === 'all');

        this.skipIndexReplicationForAllSubscription = this.skipIndexReplicationForAll.subscribe(newValue => this.toggleIndexReplication(newValue));
    }

    private getIndexReplicationStatusForAllDestinations(): string {
        var etlDestinations = this.replicationsSetup().destinations().filter(x => x.enableReplicateOnlyFromCollections());
        var etlWithSkipedIndexReplicationCount = etlDestinations.filter(x => x.skipIndexReplication()).length;

        // ReSharper disable once ConditionIsAlwaysConst
        if (etlWithSkipedIndexReplicationCount === 0)
            return 'none';

        if (etlWithSkipedIndexReplicationCount === etlDestinations.length)
            return 'all';

        return 'mixed';
    }

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            if (_.includes(db.activeBundles(), "Replication")) {
                this.replicationEnabled(true);
                this.fetchReplications(db)
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
            } else {
                this.replicationEnabled(false);
                deferred.resolve({ can: true });
            }

        }
        return deferred;
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("B8WXPY");

        var replicationSetupDirtyFlagItems = [this.replicationsSetup, this.replicationsSetup().destinations(), this.skipIndexReplicationForAll, this.replicationsSetup().clientFailoverBehaviour];

        this.replicationsSetupDirtyFlag = new ko.DirtyFlag(replicationSetupDirtyFlagItems);

        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        var combinedFlag = ko.computed(() => {
            var rs = this.replicationsSetupDirtyFlag().isDirty();
            return rs;
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag]);

        var db = this.activeDatabase();
        this.fetchCollectionsStats(db).done(results => {
            this.collections(results.collections);
        });
    }

    attached() {
        super.attached();
        $.each(this.replicationsSetup().destinations(), this.addScriptHelpPopover);
    }

    fetchReplications(db: database): JQueryPromise<any> {
        var deferred = $.Deferred();
        ko.postbox.subscribe('skip-index-replication', () => this.refereshSkipIndexReplicationForAllDestinations());

        new getReplicationsCommand(db)
            .execute()
            .done((repSetup: configurationDocumentDto<replicationsDto>) => {
                this.replicationsSetup(new replicationsSetup(repSetup));

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

    toggleSkipIndexReplicationForAll() {
        this.skipIndexReplicationForAll.toggle();
    }

    createNewDestination() {
        var db = this.activeDatabase();
        var newDestination = replicationDestination.empty(db.name);
        newDestination.enableReplicateOnlyFromCollections(true);
        newDestination.ignoredClient(true);
        newDestination.addNewCollection();
        this.replicationsSetup().destinations.unshift(newDestination);
        this.refereshSkipIndexReplicationForAllDestinations();
        this.addScriptHelpPopover();
    }

    removeDestination(repl: replicationDestination) {
        this.replicationsSetup().destinations.remove(repl);
    }

    saveChanges() {
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

    private fetchCollectionsStats(db: database): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(db).execute();
    }

    toggleIndexReplication(skipReplicationValue: boolean) {
        this.replicationsSetup().destinations().forEach(dest => {
            // since we are on ETL page filter toggle to etl destinations only
            if (dest.enableReplicateOnlyFromCollections()) {
                dest.skipIndexReplication(skipReplicationValue);
            }
        });
    }

    sendReplicateCommand(destination: replicationDestination, parentClass: etl) {
        var db = parentClass.activeDatabase();
        if (db) {
            new replicateIndexesCommand(db, destination).execute();
            new replicateTransformersCommand(db, destination).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }
    }

    enableReplication() {
        new enableReplicationCommand(this.activeDatabase())
            .execute()
            .done((bundles) => {
                var db = this.activeDatabase();
                db.activeBundles(bundles);
                this.replicationEnabled(true);
                this.fetchReplications(db);
                this.fetchCollectionsStats(db).done(results => {
                    this.collections(results.collections);
                });
            });
    }*/

    /*
        TODO @gregolsky apply google analytics
    */

}

export = etl; 
