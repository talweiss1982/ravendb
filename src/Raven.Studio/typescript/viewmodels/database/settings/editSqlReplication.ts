import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import app = require("durandal/app");
import database = require("models/resources/database");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");
import sqlReplicationStatsDialog = require("viewmodels/database/status/sqlReplicationStatsDialog");
import document = require("models/database/documents/document");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import documentMetadata = require("models/database/documents/documentMetadata");
import resetSqlReplicationCommand = require("commands/database/sqlReplication/resetSqlReplicationCommand");
import sqlReplicationSimulationDialog = require("viewmodels/database/status/sqlReplicationSimulationDialog");
import sqlReplicationConnections = require("models/database/sqlReplication/sqlReplicationConnections");
import predefinedSqlConnection = require("models/database/sqlReplication/predefinedSqlConnection");
import getSqlReplicationConnectionsCommand = require("commands/database/sqlReplication/getSqlReplicationConnectionsCommand");
import eventsCollector = require("common/eventsCollector");

class editSqlReplication extends viewModelBase {
    
    static editSqlReplicationSelector = "#editSQLReplication";
    static sqlReplicationDocumentPrefix = "Raven/SqlReplication/Configuration/";

    editedReplication = ko.observable<sqlReplication>();
    collections = ko.observableArray<string>();
    areAllSqlReplicationsValid: KnockoutComputed<boolean>;
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedSqlReplications: string[] = [];
    sqlReplicationName: KnockoutComputed<string>;
    isEditingNewReplication = ko.observable(false);
    isBasicView = ko.observable(true);
    availableConnectionStrings = ko.observableArray<predefinedSqlConnection>();
    sqlReplicationStatsAndMetricsHref = appUrl.forCurrentDatabase().statusDebugSqlReplication;
    appUrls: computedAppUrls;
    docEditor: AceAjax.Editor;
    script = ko.computed({
        read: () => {
            var r = this.editedReplication();
            return r ? r.script() : "";
        },
        write: v => this.editedReplication().script(v)
        });

    simulationDocumentId = ko.observable<string>();

    isBusy = ko.observable(false);
    initialReplicationId: string = '';

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.appUrls = appUrl.forCurrentDatabase();
        this.sqlReplicationName = ko.computed(() => (!!this.editedReplication() && !this.isEditingNewReplication()) ? this.editedReplication().name() : null);
    }

    toggleBasicMode() {
        this.isBasicView(!this.isBasicView());
    }

    private addScriptLabelPopover() {
        var popOverSettings: PopoverOptions = {
            html: true,
            trigger: 'hover',
            content: 'Replication scripts use JavaScript.<br/><br/>The script will be called once for each document in the source document collection, with <span class="code-keyword">this</span> representing the document, and the document id available as <i>documentId</i>.<br/><br/>Call <i>replicateToTableName</i> for each row you want to write to the database.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> orderData = {<br/>   Id: documentId,<br/>   OrderLinesCount: <span class="code-keyword">this</span>.Lines.length,<br/>   TotalCost: 0<br/>};<br/><br/>for (<span class="code-keyword">var</span> i = 0; i &lt; <span class="code-keyword">this</span>.Lines.length; i++) {<br/>   <span class="code-keyword">var</span> line = <span class="code-keyword">this</span>.Lines[i];<br/>   <span class="code-keyword">var</span> lineCost = ((line.Quantity * line.PricePerUnit) <br />                     * (1 - line.Discount));<br/>   orderData.TotalCost += lineCost;<br/><br/>   replicateToOrderLines({<br/>      OrderId: documentId,<br/>      Qty: line.Quantity,<br/>      Product: line.Product,<br/>      Cost: lineCost<br/>   });<br/>}<br/><br/>replicateToOrders(orderData);</pre>',
            selector: '.script-label',
            placement: "right"
        };
        $('body').popover(popOverSettings);
        $('form :input[name="ravenEntityName"]').on("keypress", (e) => e.which != 13);
    }

    loadSqlReplicationConnections(): JQueryPromise<any> {
        return new getSqlReplicationConnectionsCommand(this.activeDatabase())
            .execute()
            .done((dto: Raven.Server.Documents.SqlReplication.SqlConnections) => {
                var connections = new sqlReplicationConnections(dto);

                if (connections.predefinedConnections().length > 0) {
                    connections.predefinedConnections().forEach((x: predefinedSqlConnection) => this.availableConnectionStrings.push(x));
                }
            });
    }

    canActivate(replicationToEditName: string) {
        var canActivateResult = $.Deferred();
        this.loadSqlReplicationConnections().always(() => {
            if (replicationToEditName) {
                this.loadSqlReplication(replicationToEditName)
                    .done(() => canActivateResult.resolve({ can: true }))
                    .fail(() => {
                        messagePublisher.reportError("Could not find " + decodeURIComponent(replicationToEditName) + " replication");
                        canActivateResult.resolve({ redirect: appUrl.forSqlReplications(this.activeDatabase()) });
                    });
            } else {
                this.isEditingNewReplication(true);
                this.editedReplication(this.createSqlReplication());
                this.fetchCollections(this.activeDatabase()).always(() => canActivateResult.resolve({ can: true }));
            }
        });
        return canActivateResult;
    }

    activate(replicationToEditName: string) {
        super.activate(replicationToEditName);
        this.dirtyFlag = new ko.DirtyFlag([this.editedReplication], false, jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__metadata", "metadata"]));
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    providerChanged(obj: sqlReplication, event: JQueryEventObject) {
        if (event.originalEvent && obj.connectionStringType() == obj.CONNECTION_STRING) {
            var curConnectionString = !!this.editedReplication().connectionStringValue() ? this.editedReplication().connectionStringValue().trim() : "";
            if (curConnectionString === "" ||
                sqlReplicationConnections.sqlProvidersConnectionStrings.find(x => x.ConnectionString == curConnectionString)) {
                var matchingConnectionStringPair: { ProviderName: string; ConnectionString: string; } = sqlReplicationConnections.sqlProvidersConnectionStrings.find(x => x.ProviderName == (<any>event.originalEvent.srcElement).selectedOptions[0].value);
                if (!!matchingConnectionStringPair) {
                    var matchingConnectionStringValue: string = matchingConnectionStringPair.ConnectionString;
                    this.editedReplication().connectionStringValue(
                        matchingConnectionStringValue
                        );
                }
            }
        }
    }


    loadSqlReplication(replicationToLoadName: string) {
        var loadDeferred = $.Deferred();
        $.when(this.fetchSqlReplicationToEdit(replicationToLoadName), this.fetchCollections(this.activeDatabase()))
            .done(() => {
                this.editedReplication().collections = this.collections;
                /* TODO:
                new getDocumentsMetadataByIDPrefixCommand(editSqlReplication.sqlReplicationDocumentPrefix, 256, this.activeDatabase())
                    .execute()
                    .done((results: queryResultDto<string>) => {
                        this.loadedSqlReplications = results.Results;
                        loadDeferred.resolve();
                    }).
                    fail(() => loadDeferred.reject());*/
            })
            .fail(() => {
                loadDeferred.reject();
            });

        return loadDeferred;
    }

    fetchSqlReplicationToEdit(sqlReplicationName: string): JQueryPromise<any> {
        var loadDocTask = new getDocumentWithMetadataCommand(editSqlReplication.sqlReplicationDocumentPrefix + sqlReplicationName, this.activeDatabase()).execute();
        loadDocTask.done((document: document) => {
            var sqlReplicationDto: any = document.toDto(true);
            this.editedReplication(new sqlReplication(sqlReplicationDto));
            this.initialReplicationId = this.editedReplication().name();
            this.dirtyFlag().reset(); //Resync Changes
        });
        loadDocTask.always(() => this.isBusy(false));
        this.isBusy(true);
        return loadDocTask;
    }

    private fetchCollections(db: database): JQueryPromise<any> {
        return new getCollectionsStatsCommand(db)
            .execute()
            .done((collectionsStats: collectionsStats) => {
                this.collections(collectionsStats.collections.map(x => x.name));
            });
    }

    showStats() {
        eventsCollector.default.reportEvent("edit-sql-replication", "stats");

        var viewModel = new sqlReplicationStatsDialog(this.activeDatabase(), this.editedReplication().name());
        app.showBootstrapDialog(viewModel);
    }

    refreshSqlReplication() {
        eventsCollector.default.reportEvent("edit-sql-replicaton", "refresh");

        if (this.isEditingNewReplication() === false) {
            var docId = this.initialReplicationId;
            this.loadSqlReplication(docId);
        } else {

            this.editedReplication(this.createSqlReplication());
        }
    }

    compositionComplete() {
        super.compositionComplete();
        this.addScriptLabelPopover();
        $('pre').each((index, currentPreElement) => {
            this.initializeAceValidity(currentPreElement);
        });

        var editorElement = $("#sqlReplicationEditor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#sqlReplicationEditor").on('DynamicHeightSet', () => this.docEditor.resize());

    }

    createSqlReplication(): sqlReplication {
        eventsCollector.default.reportEvent("edit-sql-replication", "create");

        var newSqlReplication: sqlReplication = sqlReplication.empty();
        newSqlReplication.collections(this.collections());
        this.collections.subscribe(value => newSqlReplication.collections(value));
        this.subscribeToSqlReplicationName(newSqlReplication);
        return newSqlReplication;
    }


    private subscribeToSqlReplicationName(sqlReplicationElement: sqlReplication) {
        sqlReplicationElement.name.subscribe((previousName) => {
            //Get the previous value of 'name' here before it's set to newValue
            var nameInputArray = $('input[name="name"]').filter(function () { return this.value === previousName; });
            if (nameInputArray.length === 1) {
                var inputField: any = nameInputArray[0];
                inputField.setCustomValidity("");
            }
        }, this, "beforeChange");
        sqlReplicationElement.name.subscribe((newName) => {
            var message = "";
            if (newName === "") {
                message = "Please fill out this field.";
            }
            else if (this.isSqlReplicationNameExists(newName)) {
                message = "SQL Replication name already exists.";
            }
            $('input[name="name"]')
                .filter(function () { return this.value === newName; })
                .each((index: number, element: any) => {
                    element.setCustomValidity(message);
                });
        });
    }

    detached() {
        super.detached();
        $("#sqlReplicationEditor").off('DynamicHeightSet');
    }


    private isSqlReplicationNameExists(name: string): boolean {
        return !!this.loadedSqlReplications.find(x => x === name);
    }

    private initializeAceValidity(element: Element) {
        var editorElement = $("#aceEditor");
        if (editorElement.length > 0) {
            var editor = ko.utils.domData.get(editorElement[0], "aceEditor");
            var editorValue = editor.getSession().getValue();
            if (editorValue === "") {
                var textarea: any = $(element).find('textarea')[0];
                textarea.setCustomValidity("Please fill out this field.");
            }
        }
    }

    save() {
        eventsCollector.default.reportEvent("edit-sql-replication", "save");

        var currentDocumentId = this.editedReplication().name();
        this.editedReplication().script(this.script());

        if (this.initialReplicationId !== currentDocumentId) {
            this.editedReplication().__metadata.etag(undefined);
            delete this.editedReplication().__metadata.lastModified;
        }

        var newDoc = new document(this.editedReplication().toDto());
        newDoc.__metadata = new documentMetadata();
        this.attachReservedMetaProperties(editSqlReplication.sqlReplicationDocumentPrefix + currentDocumentId, newDoc.__metadata);

        var saveCommand = new saveDocumentCommand(editSqlReplication.sqlReplicationDocumentPrefix + currentDocumentId, newDoc, this.activeDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((saveResult: saveDocumentResponseDto) => {
            var savedDocumentDto: saveDocumentResponseItemDto = saveResult.Results[0];
            var sqlReplicationKey = savedDocumentDto.Key.substring(editSqlReplication.sqlReplicationDocumentPrefix.length);
            this.loadSqlReplication(sqlReplicationKey)
                .done(() => this.dirtyFlag().reset());
            this.updateUrl(sqlReplicationKey);

            this.isEditingNewReplication(false);
            this.initialReplicationId = currentDocumentId;
        });
    }


    updateUrl(docId: string) {
        var url = appUrl.forEditSqlReplication(docId, this.activeDatabase());
        router.navigate(url, false);
    }

    attachReservedMetaProperties(id: string, target: documentMetadata) {
        //TODO: target.etag = '';
        target.collection = target.collection || document.getCollectionFromId(id);
        target.id = id;
    }

    deleteSqlReplication() {
        eventsCollector.default.reportEvent("edit-sql-replication", "delete");
        var newDoc = new document(this.editedReplication().toDto());

        if (newDoc) {
            var viewModel = new deleteDocuments([newDoc], this.activeDatabase());
            viewModel.deletionTask.done(() => {
                this.dirtyFlag().reset(); //Resync Changes
                router.navigate(appUrl.forCurrentDatabase().sqlReplications());
            });
            app.showBootstrapDialog(viewModel, editSqlReplication.editSqlReplicationSelector);

        }
    }
    resetSqlReplication() {
        eventsCollector.default.reportEvent("edit-sql-replication", "reset");

        app.showBootstrapMessage("You are about to reset this SQL Replication, forcing replication of all collection items", "SQL Replication Reset", ["Cancel", "Reset"])
            .then((dialogResult: string) => {
                if (dialogResult === "Reset") {
                    var replicationId = this.initialReplicationId;
                    new resetSqlReplicationCommand(this.activeDatabase(), replicationId).execute()
                        .done(() => messagePublisher.reportSuccess("SQL replication " + replicationId + " was reset successfully!"))
                        .fail(() => messagePublisher.reportError("SQL replication " + replicationId + " failed to reset!"));
                }
            });

    }

    simulateSqlReplication() {
        eventsCollector.default.reportEvent("edit-sql-replication", "simulate");

        this.editedReplication().script(this.script());
        var viewModel = new sqlReplicationSimulationDialog(this.activeDatabase(), this.editedReplication(), this.simulationDocumentId);
        app.showBootstrapDialog(viewModel);
    }

    getSqlReplicationConnectionStringsUrl(sqlReplicationName: string) {
        return appUrl.forSqlReplicationConnections(this.activeDatabase());
    }

}

export = editSqlReplication; 
