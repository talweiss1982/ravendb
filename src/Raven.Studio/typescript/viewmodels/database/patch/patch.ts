import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/database/patch/patchDocument");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import jsonUtil = require("common/jsonUtil");
import appUrl = require("common/appUrl");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import savePatch = require('viewmodels/database/patch/savePatch');
import executePatchConfirm = require('viewmodels/database/patch/executePatchConfirm');
import savePatchCommand = require('commands/database/patch/savePatchCommand');
import executePatchCommand = require("commands/database/patch/executePatchCommand");
import evalByQueryCommand = require("commands/database/patch/evalByQueryCommand");
import evalByCollectionCommand = require("commands/database/patch/evalByCollectionCommand");
import documentMetadata = require("models/database/documents/documentMetadata");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import queryUtil = require("common/queryUtil");
import recentPatchesStorage = require("common/storage/recentPatchesStorage");
import getPatchesCommand = require('commands/database/patch/getPatchesCommand');
import killOperationComamnd = require('commands/operations/killOperationCommand');
import eventsCollector = require("common/eventsCollector");
import notificationCenter = require("common/notifications/notificationCenter");
import genUtils = require("common/generalUtils");
import queryCriteria = require("models/database/query/queryCriteria");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
type indexInfo = {
    name: string;
    isMapReduce: boolean;
}

type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<document>>;

class patch extends viewModelBase {

    gridController = ko.observable<virtualGridController<document>>();
    private fetcher = ko.observable<fetcherType>();

    displayName = "patch";
    indices = ko.observableArray<indexInfo>([]);
    indicesToSelect: KnockoutComputed<indexInfo[]>;
    collections = ko.observableArray<collection>([]);
    collectionToSelect: KnockoutComputed<collection[]>;

    recentPatches = ko.observableArray<storedPatchDto>();
    savedPatches = ko.observableArray<patchDocument>();

    showDocumentsPreview: KnockoutObservable<boolean>;

    patchDocument = ko.observable<patchDocument>();

    beforePatch: KnockoutComputed<string>;
    beforePatchDoc = ko.observable<string>();
    beforePatchMeta = ko.observable<string>();
    beforePatchDocMode = ko.observable<boolean>(true);
    beforePatchEditor: AceAjax.Editor;

    afterPatch = ko.observable<string>();
    afterPatchDoc = ko.observable<string>();
    afterPatchMeta = ko.observable<string>();
    afterPatchDocMode = ko.observable<boolean>(true);
    afterPatchEditor: AceAjax.Editor;

    loadedDocuments = ko.observableArray<string>();
    putDocuments = ko.observableArray<any>();
    outputLog = ko.observableArray<string>();

    runningPatchesCount = ko.observable<number>();
    runningTasksUrl = ko.computed(() => appUrl.forRunningTasks(this.activeDatabase()));
    runningPatchesText = ko.computed(() => {
        var count = this.runningPatchesCount();
        if (count > 1) {
            return count + ' patches in progress';
        }
        return count + ' patch in progress';
    });
    runningPatchesPollingHandle: number;

    isExecuteAllowed: KnockoutComputed<boolean>;
    isMapReduceIndexSelected: KnockoutComputed<boolean>;
    documentKey = ko.observable<string>();
    keyOfTestedDocument: KnockoutComputed<string>;

    isPatchingInProgress = ko.observable<boolean>(false);
    showPatchingProgress = ko.observable<boolean>(false);
    patchOperationId = ko.observable<number>();
    patchingProgress = ko.observable<number>(0);
    patchingProgressPercentage: KnockoutComputed<string>;
    patchingProgressText = ko.observable<string>();
    patchSuccess = ko.observable<boolean>(false);
    patchFailure = ko.observable<boolean>(false);
    patchKillInProgress = ko.observable<boolean>(false);

    static gridSelector = "#matchingDocumentsGrid";

    constructor() {
        super();

        aceEditorBindingHandler.install();

        // When we programmatically change the document text or meta text, push it into the editor.
        this.beforePatchDocMode.subscribe(() => {
            if (this.beforePatchEditor) {
                var text = this.beforePatchDocMode() ? this.beforePatchDoc() : this.beforePatchMeta();
                this.beforePatchEditor.getSession().setValue(text);
            }
        });
        this.beforePatch = ko.computed({
            read: () => {
                return this.beforePatchDocMode() ? this.beforePatchDoc() : this.beforePatchMeta();
            },
            write: (text: string) => {
                var currentObservable = this.beforePatchDocMode() ? this.beforePatchDoc : this.beforePatchMeta;
                currentObservable(text);
            },
            owner: this
        });

        this.afterPatchDocMode.subscribe(() => {
            if (this.afterPatchEditor) {
                var text = this.afterPatchDocMode() ? this.afterPatchDoc() : this.afterPatchMeta();
                this.afterPatchEditor.getSession().setValue(text);
            }
        });
        this.afterPatch = ko.computed({
            read: () => {
                return this.afterPatchDocMode() ? this.afterPatchDoc() : this.afterPatchMeta();
            },
            write: (text: string) => {
                var currentObservable = this.afterPatchDocMode() ? this.afterPatchDoc : this.afterPatchMeta;
                currentObservable(text);
            },
            owner: this
        });

        // Refetch the index fields whenever the selected index name changes.
        this.selectedIndex
            .subscribe(indexName => {
                if (indexName) {
                    this.fetchIndexFields(indexName);
                }
            });

        this.indicesToSelect = ko.computed(() => {
            var indicies = this.indices();
            var patchDocument = this.patchDocument();
            if (indicies.length === 0 || !patchDocument)
                return [];

            return indicies.filter(x => x.name !== patchDocument.selectedItem());
        });

        this.collectionToSelect = ko.computed(() => {
            var collections = this.collections();
            var patchDocument = this.patchDocument();
            if (collections.length === 0 || !patchDocument)
                return [];

            return collections.filter((x: collection) => x.name !== patchDocument.selectedItem());
        });

        this.showDocumentsPreview = ko.computed(() => {
            if (!this.patchDocument()) {
                return false;
            }
            var indexPath = this.patchDocument().isIndexPatch();
            var collectionPath = this.patchDocument().isCollectionPatch();
            return indexPath || collectionPath;
        });
    }

    compositionComplete() {
        super.compositionComplete();

        var beforePatchEditorElement = $("#beforePatchEditor");
        if (beforePatchEditorElement.length > 0) {
            this.beforePatchEditor = ko.utils.domData.get(beforePatchEditorElement[0], "aceEditor");
        }

        var afterPatchEditorElement = $("#afterPatchEditor");
        if (afterPatchEditorElement.length > 0) {
            this.afterPatchEditor = ko.utils.domData.get(afterPatchEditorElement[0], "aceEditor");
        }

        const grid = this.gridController();
        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.collections().map(x => x.name), {
            showRowSelectionCheckbox: true,
            showSelectAllCheckbox: false
        });

        const fakeFetcher: fetcherType = (s, t) => $.Deferred<pagedResult<document>>().resolve({
            items: [],
            totalResultCount: 0
        });

        grid.headerVisible(true);
        grid.init((s, t) => this.fetcher() ? this.fetcher()(s, t) : fakeFetcher(s, t), (w, r) => documentsProvider.findColumns(w, r));

        this.fetcher.subscribe(() => grid.reset());

        grid.selection.subscribe(selection => {
            if (selection.count === 1) {
                var document = selection.included[0];
                // load document directly from server as documents on list are loaded using doc-preview endpoint, which doesn't display entire document
                this.loadDocumentToTest(document.__metadata.id);
                this.documentKey(document.__metadata.id);
            } else {
                this.clearDocumentPreview();
            }
        });

        //TODO: install doc preview tooltip
    }

    activate(recentPatchHash?: string) {
        super.activate(recentPatchHash);
        this.updateHelpLink("QGGJR5");
        this.patchDocument(patchDocument.empty());
        this.queryText.throttle(1000).subscribe(() => {
            this.runQuery();
        });

        this.isExecuteAllowed = ko.computed(() => !!this.patchDocument().script() && !!this.beforePatchDoc());
        this.isMapReduceIndexSelected = ko.computed(() => {
            if (this.patchDocument().patchOnOption() !== "Index") {
                return false;
            }
            var indexName = this.selectedIndex();
            var usedIndex = this.indices().find(x => x.name === indexName);
            if (usedIndex) { 
                return usedIndex.isMapReduce;
            }
            return false;
        })
        this.keyOfTestedDocument = ko.computed(() => {
            switch (this.patchDocument().patchOnOption()) {
                case "Collection":
                case "Index":
                    return this.documentKey();
                case "Document":
                    return this.patchDocument().selectedItem();
            }
        });

        this.patchingProgressPercentage = ko.computed(() => this.patchingProgress() + "%");

        var db = this.activeDatabase();
        if (!!db) {
            this.fetchRecentPatches();
            this.fetchAllPatches();
            this.fetchRunningPatches();
        }

        if (recentPatchHash) {
            this.selectInitialPatch(recentPatchHash);
        }
    }

    attached() {
        super.attached();
        $("#indexQueryLabel").popover({
            html: true,
            trigger: "hover",
            container: '.form-horizontal',
            content: '<p>Queries use Lucene syntax. Examples:</p><pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>'
        });
        $("#patchScriptsLabel").popover({
            html: true,
            trigger: "hover",
            container: ".form-horizontal",
            content: '<p>Patch Scripts are written in JavaScript. Examples:</p><pre><span class="code-keyword">this</span>.NewProperty = <span class="code-keyword">this</span>.OldProperty + myParameter;<br/><span class="code-keyword">delete this</span>.UnwantedProperty;<br/><span class="code-keyword">this</span>.Comments.RemoveWhere(<span class="code-keyword">function</span>(comment){<br/>  <span class="code-keyword">return</span> comment.Spam;<br/>});</pre>'
        });

        var rowCreatedEvent = app.on(patch.gridSelector + 'RowsCreated').then(() => {
            rowCreatedEvent.off();
        });

        this.registerDisposableHandler($(window), 'storage', () => {
            this.fetchRecentPatches();
        });
    }

    private fetchAllPatches() {
        new getPatchesCommand(this.activeDatabase())
            .execute()
            .done((patches: patchDocument[]) => this.savedPatches(patches));
    }

    private fetchRunningPatches() {
        if (this.runningPatchesPollingHandle)
            return;

        /* TODO:
        new getOperationsCommand(this.activeDatabase())
            .execute()
            .done((tasks: runningTaskDto[]) => {
                var count = tasks.filter(x => x.TaskType === "IndexBulkOperation" && !x.Completed).length;
                this.runningPatchesCount(count);

                // we enable polling only if at least one patch is in progress
                if (count > 0) {
                    this.runningPatchesPollingHandle = setTimeout(() => {
                        this.runningPatchesPollingHandle = null;
                        this.fetchRunningPatches();
                    }, 5000);
                } else {
                    this.runningPatchesPollingHandle = null;
                }
            });*/
    }

    private fetchRecentPatches() {
        recentPatchesStorage.getRecentPatchesWithIndexNamesCheck(this.activeDatabase())
            .done(result => {
                this.recentPatches(result);
            });
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    selectInitialPatch(recentPatchHash: string) {
        if (recentPatchHash.indexOf("recentpatch-") === 0) {
            var hash = parseInt(recentPatchHash.substr("recentpatch-".length), 10);
            var matchingPatch = this.recentPatches().find(q => q.Hash === hash);
            if (matchingPatch) {
                this.useRecentPatch(matchingPatch);
            } else {
                this.navigate(appUrl.forPatch(this.activeDatabase()));
            }
        }
    }

    loadDocumentToTest(selectedItem: string) {
        if (selectedItem) {
            var loadDocTask = new getDocumentWithMetadataCommand(selectedItem, this.activeDatabase()).execute();
            loadDocTask.done(document => {
                this.beforePatchDoc(JSON.stringify(document.toDto(), null, 4));
                this.beforePatchMeta(JSON.stringify(documentMetadata.filterMetadata(document.__metadata.toDto()), null, 4));
            }).fail(() => this.clearDocumentPreview());
        } else {
            this.clearDocumentPreview();
        }
    }

    private clearDocumentPreview() {
        this.beforePatchDoc("");
        this.beforePatchMeta("");
        this.afterPatchDoc("");
        this.afterPatchMeta("");
        this.putDocuments([]);
        this.loadedDocuments([]);
        this.outputLog([]);
    }

    setSelectedPatchOnOption(patchOnOption: string) {
        this.resetProgressBar();
        this.patchDocument().patchOnOption(patchOnOption);
        this.patchDocument().selectedItem('');
        this.clearDocumentPreview();
        switch (patchOnOption) {
            case "Collection":
                this.fetchAllCollections();
                break;
            case "Index":
                this.fetchAllIndexes()
                    .done(() => this.runQuery());
                $("#matchingDocumentsGrid").resize();
                break;
            default:
                this.fetcher(null);
                break;
        }
    }

    fetchAllCollections(): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: collectionsStats) => {
                const colls = stats.collections;
                var currentlySelectedCollection: collection = null;

                if (this.patchDocument().selectedItem()) {
                    var selected = this.patchDocument().selectedItem();
                    currentlySelectedCollection = this.collections().find(c => c.name === selected);
                }

                this.collections(colls);
                if (this.collections().length > 0) {
                    this.setSelectedCollection(currentlySelectedCollection || this.collections()[0]);
                }
            });
    }

    setSelectedCollection(coll: collection) {
        this.resetProgressBar();
        this.patchDocument().selectedItem(coll.name);

        var fetcher = (skip: number, take: number) => coll.fetchDocuments(skip, take);
        this.fetcher(fetcher);
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results) => {
                this.indices(results.Indexes.map(i => {
                    return {
                        name: i.Name,
                        isMapReduce: false, //TODO: i.IsMapReduce
                    }
                }));
                if (this.indices().length > 0) {
                    this.setSelectedIndex(this.indices()[0].name);
                }
            });
    }

    setSelectedIndex(indexName: string) {
        this.resetProgressBar();
        this.selectedIndex(indexName);
        this.patchDocument().selectedItem(indexName);
    }

    useIndex(indexName: string) {
        this.setSelectedIndex(indexName);
        this.runQuery();
    }

    runQuery(): void {
        var selectedIndex = this.patchDocument().selectedItem();
        if (selectedIndex) {
            var queryText = this.queryText();
            this.patchDocument().query(queryText);
            var database = this.activeDatabase();
            var resultsFetcher = (skip: number, take: number) => {

                const criteria = queryCriteria.empty();
                criteria.selectedIndex(selectedIndex);
                criteria.queryText(queryText);

                var command = new queryIndexCommand(database, skip, take, criteria);
                return command.execute();
            };
            this.fetcher(resultsFetcher);
        }
    }

    savePatch() {
        eventsCollector.default.reportEvent("patch", "save");

        var savePatchViewModel: savePatch = new savePatch();
        app.showBootstrapDialog(savePatchViewModel);
        savePatchViewModel.onExit().done((patchName) => {
            new savePatchCommand(patchName, this.patchDocument(), this.activeDatabase())
                .execute()
                .done(() => this.fetchAllPatches());
        });
    }

    private usePatch(patch: patchDocument) {
        this.clearDocumentPreview();
        var selectedItem = patch.selectedItem();
        patch = patch.clone();
        patch.resetMetadata();
        this.patchDocument(patch);
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                this.fetchAllCollections().then(() => {
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === selectedItem))[0]);
                });
                break;
            case "Index":
                this.fetchAllIndexes().then(() => {
                    this.setSelectedIndex(selectedItem);
                    this.queryText(patch.query());
                    this.runQuery();
                });
                break;
            case "Document":
                this.loadDocumentToTest(patch.selectedItem());
                break;
        }
    }

    testPatch() {
        eventsCollector.default.reportEvent("patch", "test");

        var values: dictionary<string> = {};
        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });
        var bulkDocs: Array<bulkDocumentDto> = [];
        bulkDocs.push({
            Key: this.keyOfTestedDocument(),
            Method: 'PATCH',
            DebugMode: true,
            Patch: {
                Script: this.patchDocument().script(),
                Values: values
            }
        });
        new executePatchCommand(bulkDocs, this.activeDatabase(), true)
            .execute()
            .done((result: bulkDocumentDto[]) => {
                var testResult = new document((<any>result).Results[0].AdditionalData['Document']);
                this.afterPatchDoc(JSON.stringify(testResult.toDto(), null, 4));
                this.afterPatchMeta(JSON.stringify(documentMetadata.filterMetadata(testResult.__metadata.toDto()), null, 4));
                this.updateActions((<any>result).Results[0].AdditionalData['Actions']);
                this.outputLog((<any>result).Results[0].AdditionalData["Debug"]);
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));
        this.recordPatchRun();
    }

    private updatePageUrl(hash: number) {
        // Put the patch into the URL, so that if the user refreshes the page, he's still got this patch loaded.
        var queryUrl = appUrl.forPatch(this.activeDatabase(), hash);
        this.updateUrl(queryUrl);
    }

    recordPatchRun() {
        var patchDocument = this.patchDocument();

        var newPatch = <storedPatchDto>patchDocument.toDto();
        delete newPatch["@metadata"];
        newPatch.Hash = 0;

        var stringForHash = newPatch.PatchOnOption + newPatch.SelectedItem + newPatch.Script + newPatch.Values;

        if (patchDocument.patchOnOption() === "Index") {
            newPatch.Query = this.queryText();
            stringForHash += newPatch.Query;
        }

        newPatch.Hash = genUtils.hashCode(stringForHash);

        this.updatePageUrl(newPatch.Hash);

        // Add this query to our recent patches list in the UI, or move it to the top of the list if it's already there.
        var existing = this.recentPatches().find(q => q.Hash === newPatch.Hash);
        if (existing) {
            this.recentPatches.remove(existing);
            this.recentPatches.unshift(existing);
        } else {
            this.recentPatches.unshift(newPatch);
        }

        // Limit us to 15 recent patchs
        if (this.recentPatches().length > 15) {
            this.recentPatches.remove(this.recentPatches()[15]);
        }

        //save the recent queries to local storage
        recentPatchesStorage.saveRecentPatches(this.activeDatabase(), this.recentPatches());
    }

    useRecentPatch(patchToUse: storedPatchDto) {
        eventsCollector.default.reportEvent("patch", "use-recent");
        var patchDoc = new patchDocument(patchToUse);
        this.usePatch(patchDoc);
    }

    private updateActions(actions: { PutDocument: any[]; LoadDocument: any }) {
        this.loadedDocuments(actions.LoadDocument || []);
        this.putDocuments((actions.PutDocument || []).map(doc => jsonUtil.syntaxHighlight(doc)));
    }

    executePatchOnSingle() {
        eventsCollector.default.reportEvent("patch", "run", "single");
        var keys: string[] = [];
        keys.push(this.patchDocument().selectedItem());
        this.confirmAndExecutePatch(keys);
    }

    executePatchOnSelected() {
        eventsCollector.default.reportEvent("patch", "run", "selected");
        this.confirmAndExecutePatch(this.gridController().selection().included.map(x => x.getId()));
    }

    executePatchOnAll() {
        eventsCollector.default.reportEvent("patch", "run", "all");
        var confirmExec = new executePatchConfirm();
        confirmExec.viewTask.done(() => {
            this.executePatchAllCommand()
                .done((result: operationIdDto) => this.onPatchAllScheduled(result));

            this.recordPatchRun();
        });
        app.showBootstrapDialog(confirmExec);
    }

    private executePatchAllCommand(): JQueryPromise<operationIdDto> {

        this.patchSuccess(false);
        this.patchFailure(false);
        const values: dictionary<string> = {};

        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });

        const patch = {
            Script: this.patchDocument().script(),
            Values: values
        } as Raven.Server.Documents.Patch.PatchRequest;

        const patchOption = this.patchDocument().patchOnOption();

        if (patchOption === "Collection") {
            const collectionToPatch = this.patchDocument().selectedItem();
            return new evalByCollectionCommand(collectionToPatch, patch, this.activeDatabase()).execute();
        } else if (patchOption === "Index") {
            const index = this.patchDocument().selectedItem();
            const query = this.patchDocument().query();
            return new evalByQueryCommand(index, query, patch, this.activeDatabase()).execute();
        } else {
            throw new Error("Unhandled patch option: " + patchOption);
        }
    }

    private onPatchAllScheduled(result: operationIdDto) {
        this.resetProgressBar();
        this.isPatchingInProgress(true);
        this.showPatchingProgress(true);
        // TODO: this.fetchRunningPatches();

        notificationCenter.instance.monitorOperation(this.activeDatabase(), result.OperationId)
            .done(() => {
                this.patchSuccess(true);
                this.patchKillInProgress(false);
                this.isPatchingInProgress(false);
            });
    }

    private resetProgressBar() {
        this.showPatchingProgress(false);     
        this.patchingProgress(0);
        this.patchingProgressText("");
    }

    private updateProgress(status: bulkOperationStatusDto) {

        if (status.OperationProgress != null) {
            var progressValue = Math.round(100 * (status.OperationProgress.ProcessedEntries / status.OperationProgress.TotalEntries));
            this.patchingProgress(progressValue);
            var progressPrefix = "";
            if (status.Completed) {
                if (status.Canceled) {
                    progressPrefix = "Patch canceled: ";
                    this.patchFailure(true);
                } else if (status.Faulted) {
                    progressPrefix = "Patch failed: ";
                    this.patchFailure(true);
                } else {
                    progressPrefix = "Patch completed: ";
                    this.patchSuccess(true);
                }
            }
            if (status.OperationProgress.TotalEntries) {
                this.patchingProgressText(progressPrefix + status.OperationProgress.ProcessedEntries.toLocaleString() + " / " + status.OperationProgress.TotalEntries.toLocaleString() + " (" + progressValue + "%)");    
            }
        }

        if (status.Completed) {
            this.patchKillInProgress(false);
            this.isPatchingInProgress(false);
        }
    }

    private confirmAndExecutePatch(keys: string[]) {
        var confirmExec = new executePatchConfirm();
        confirmExec.viewTask.done(() => this.executePatch(keys));
        app.showBootstrapDialog(confirmExec);
    }

    private executePatch(keys: string[]) {
        var values: dictionary<string> = {};
        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });
        var bulkDocs: Array<bulkDocumentDto> = [];
        keys.forEach(
            key => bulkDocs.push({
                Key: key,
                Method: 'PATCH',
                DebugMode: false,
                Patch: {
                    Script: this.patchDocument().script(),
                    Values: values
                }
            })
        );
        new executePatchCommand(bulkDocs, this.activeDatabase(), false)
            .execute()
            .done((result: bulkDocumentDto[]) => {
                this.afterPatchDoc("");
                this.afterPatchMeta("");
                if (this.patchDocument().patchOnOption() === 'Document') {
                    this.loadDocumentToTest(this.patchDocument().selectedItem());
                }
                this.updateDocumentsList();
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));

        this.recordPatchRun();
    }

    private updateDocumentsList() {
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                this.fetchAllCollections().then(() => {
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === this.patchDocument().selectedItem()))[0]);
                });
                break;
            case "Index":
                this.useIndex(this.patchDocument().selectedItem());
                break;
        }
    }

    activateBeforeDoc() {
        this.beforePatchDocMode(true);
    }

    activateBeforeMeta() {
        this.beforePatchDocMode(false);
    }

    activateAfterDoc() {
        this.afterPatchDocMode(true);
    }

    activateAfterMeta() {
        this.afterPatchDocMode(false);
    }

    indexFields = ko.observableArray<string>();
    selectedIndex = ko.observable<string>();
    dynamicPrefix = "dynamic/";
    isTestIndex = ko.observable<boolean>(false);
    queryText = ko.observable("");

    queryCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {

        queryUtil.queryCompleter(this.indexFields, this.selectedIndex, this.dynamicPrefix, this.activeDatabase, editor, session, pos, prefix, callback);

    }

    killPatch() {
        var operationToKill = this.patchOperationId();
        if (operationToKill) {
            this.confirmationMessage("Are you sure?", "You are stopping patch execution.")
                .done(() => {
                    if (this.patchOperationId()) {
                        new killOperationComamnd(this.activeDatabase(), operationToKill)
                            .execute()
                            .done(() => {
                                if (this.patchOperationId()) {
                                    this.patchKillInProgress(true);
                                }
                            });
                        eventsCollector.default.reportEvent("patch", "kill");
                    }
                });
        }
    }

    fetchIndexFields(indexName: string) {
        // Fetch the index definition so that we get an updated list of fields to be used as sort by options.
        // Fields don't show for All Documents.
        var self = this;
        var isAllDocumentsDynamicQuery = indexName === "All Documents";
        if (!isAllDocumentsDynamicQuery) {
            //if index is dynamic, get columns using index definition, else get it using first index result
            if (indexName.indexOf(this.dynamicPrefix) === 0) {
                var collectionName = indexName.substring(8);
                new collection(collectionName, this.activeDatabase())
                    .fetchDocuments(0, 1)
                    .done((result: pagedResult<any>) => {
                        if (!!result && result.totalResultCount > 0 && result.items.length > 0) {
                            var dynamicIndexPattern: document = new document(result.items[0]);
                            if (!!dynamicIndexPattern) {
                                this.indexFields(dynamicIndexPattern.getDocumentPropertyNames());
                            }
                        }
                    });
            } else {
                new getIndexDefinitionCommand(indexName, this.activeDatabase())
                    .execute()
                    .done((result) => {
                        self.isTestIndex(result.IsTestIndex);
                        self.indexFields(Object.keys(result.Fields));
                    });
            }
        }
    }
}

export = patch;
