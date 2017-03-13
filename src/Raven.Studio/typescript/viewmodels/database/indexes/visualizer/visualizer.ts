import viewModelBase = require("viewmodels/viewModelBase");
import visualizerGraphGlobal = require("viewmodels/database/indexes/visualizer/visualizerGraphGlobal");
import visualizerGraphDetails = require("viewmodels/database/indexes/visualizer/visualizerGraphDetails");

import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import getIndexMapReduceTreeCommand = require("commands/database/index/getIndexMapReduceTreeCommand");
import getIndexDebugSourceDocumentsCommand = require("commands/database/index/getIndexDebugSourceDocumentsCommand");

class visualizer extends viewModelBase {

    static readonly noIndexSelected = "Select an index";

    indexes = ko.observableArray<string>();
    indexName = ko.observable<string>();
    
    private currentIndex = ko.observable<string>();

    private currentIndexUi: KnockoutComputed<string>;
    private hasIndexSelected: KnockoutComputed<boolean>;

    private documents = {
        docKey: ko.observable(""),
        hasFocusDocKey: ko.observable<boolean>(false),
        loadingDocKeySearchResults: ko.observable<boolean>(false), //TODO: autocomplete support
        docKeys: ko.observableArray<string>(),
        docKeysSearchResults: ko.observableArray<string>()
    }

    private trees = [] as Raven.Server.Documents.Indexes.Debugging.ReduceTree[];

    private globalGraph = new visualizerGraphGlobal();
    private detailsGraph = new visualizerGraphDetails();

    constructor() {
        super();

        this.bindToCurrentInstance("setSelectedIndex", "selectDocKey", "addCurrentDocumentKey");

        this.initObservables();
    }

    private initObservables() {
        this.currentIndexUi = ko.pureComputed(() => {
            const currentIndex = this.currentIndex();
            return currentIndex || visualizer.noIndexSelected;
        });

        this.hasIndexSelected = ko.pureComputed(() => !!this.currentIndex());

        this.documents.hasFocusDocKey.subscribe(value => {
            if (!value) {
                return;
            }
            this.fetchDocKeySearchResults("");
        });

        this.documents.docKey.throttle(100).subscribe(query => this.fetchDocKeySearchResults(query));
    }

    activate(args: any) {
        return new getIndexesStatsCommand(this.activeDatabase())
            .execute()
            .done(result => this.onIndexesLoaded(result));
    }

    compositionComplete() {
        super.compositionComplete();

        this.globalGraph.init((treeName: string) => this.detailsGraph.openFor(treeName));
        this.detailsGraph.init(() => this.globalGraph.restoreView(), this.trees);
    }

    private onIndexesLoaded(indexes: Raven.Client.Documents.Indexes.IndexStats[]) {
        this.indexes(indexes.filter(x => x.Type === "AutoMapReduce" || x.Type === "MapReduce").map(x => x.Name));
    }

    setSelectedIndex(indexName: string) {
        this.currentIndex(indexName);

        this.resetGraph();
    }

    private resetGraph() {
        this.documents.docKeys([]);
        this.documents.docKey("");
        this.documents.docKeysSearchResults([]);
        
        this.globalGraph.reset();
        this.detailsGraph.reset();
    }

    addCurrentDocumentKey() {
        this.addDocKey(this.documents.docKey());
    }

    private addDocKey(key: string) {
        if (!key) {
            return;
        }

        if (_.includes(this.documents.docKeys(), key)) {
            this.globalGraph.zoomToDocument(key);
        } else {
            //TODO: spinner
            new getIndexMapReduceTreeCommand(this.activeDatabase(), this.currentIndex(), key)
                .execute()
                .done((mapReduceTrees) => {
                    if (!_.includes(this.documents.docKeys(), key)) {
                        this.documents.docKeys.push(key);

                        this.addDocument(key);
                        this.addTrees(mapReduceTrees);

                        this.globalGraph.zoomToDocument(key);
                    }
                });
        }
    }

    private addDocument(docName: string) {       
        this.globalGraph.addDocument(docName);
        this.detailsGraph.addDocument(docName);
    }

    private addTrees(result: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        const treesToAdd = [] as Raven.Server.Documents.Indexes.Debugging.ReduceTree[];

        for (let i = 0; i < result.length; i++) {
            const incomingTree = result[i];

            const existingTree = this.trees.find(x => x.Name === incomingTree.Name);

            if (existingTree) {
                this.mergeTrees(incomingTree, existingTree);
                treesToAdd.push(existingTree);
            } else {
                treesToAdd.push(incomingTree);
                this.trees.push(incomingTree);
            }
        }

        this.globalGraph.addTrees(treesToAdd);
        this.detailsGraph.setDocumentsColors(this.globalGraph.getDocumentsColors());
    }

    private mergeTrees(incoming: Raven.Server.Documents.Indexes.Debugging.ReduceTree, mergeOnto: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        if (incoming.PageCount !== mergeOnto.PageCount || incoming.NumberOfEntries !== mergeOnto.NumberOfEntries) {
            throw new Error("Looks like tree data was changed. Can't render graph");
        }

        const existingLeafs = visualizer.extractLeafs(mergeOnto.Root);
        const newLeafs = visualizer.extractLeafs(incoming.Root);

        existingLeafs.forEach((page, pageNumber) => {
            const newPage = newLeafs.get(pageNumber);

            for (let i = 0; i < newPage.Entries.length; i++) {
                if (newPage.Entries[i].Source) {
                    page.Entries[i].Source = newPage.Entries[i].Source;
                }
            }
        });
    }

    private static extractLeafs(root: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage): Map<number, Raven.Server.Documents.Indexes.Debugging.ReduceTreePage> {
        const result = new Map<number, Raven.Server.Documents.Indexes.Debugging.ReduceTreePage>();

        const visitor = (node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) => {

            if (node.Entries && node.Entries.length) {
                result.set(node.PageNumber, node);
            }

            if (node.Children) {
                for (let i = 0; i < node.Children.length; i++) {
                    visitor(node.Children[i]);
                }
            }
        }

        visitor(root);

        return result;
    }

    selectDocKey(value: string) {
        this.addDocKey(value);
        this.documents.docKey("");
        this.documents.docKeysSearchResults.removeAll();
    }

    private fetchDocKeySearchResults(query: string) {
        this.documents.loadingDocKeySearchResults(true);

        new getIndexDebugSourceDocumentsCommand(this.activeDatabase(), this.currentIndex(), query, 0, 10)
            .execute()
            .done(result => {
                if (this.documents.docKey() === query) {
                    this.documents.docKeysSearchResults(result.Results);
                }
            })
            .always(() => this.documents.loadingDocKeySearchResults(false));
    }

    /*
        TODO @gregolsky apply google analytics
    */
}

export = visualizer;
