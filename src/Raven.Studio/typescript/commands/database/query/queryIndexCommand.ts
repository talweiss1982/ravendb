import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");
import queryCriteria = require("models/database/query/queryCriteria");

class queryIndexCommand extends commandBase {
    constructor(private db: database, private skip: number, private take: number, private criteria: queryCriteria, private disableCache?: boolean) {
        super();
    }

    execute(): JQueryPromise<pagedResult<document>> {
        const selector = (results: Raven.Client.Documents.Queries.QueryResult<Array<any>>) =>
            ({ items: results.Results.map(d => new document(d)), totalResultCount: results.TotalResults, additionalResultInfo: results, resultEtag: results.ResultEtag.toString() }) as pagedResult<document>
        return this.query(this.getUrl(), null, this.db, selector)
            .fail((response: JQueryXHR) => this.reportError("Error querying index", response.responseText, response.statusText));
    }

    getUrl() {
        const criteria = this.criteria;
        const url = endpoints.databases.queries.queries$ + criteria.selectedIndex();
        const resultsTransformerUrlFragment = criteria.getTransformerQueryUrlPart();

        const urlArgs = this.urlEncodeArgs({
            query: criteria.queryText() || undefined,
            start: this.skip,
            pageSize: this.take,
            sort: criteria.sorts().filter(x => x.fieldName()).map(x => x.toQuerySortString()),
            fetch: criteria.showFields() ? "__all_fields" : undefined,
            debug: criteria.indexEntries() ? "entries" : undefined,
            operator: criteria.useAndOperator() ? "AND" : undefined, 
            disableCache: this.disableCache ? Date.now() : undefined
        }) + resultsTransformerUrlFragment;
        return url + urlArgs;
    }

    getCsvUrl() {
        const criteria = this.criteria;
        const url = endpoints.databases.streaming.streamsQueries$ + criteria.selectedIndex();
        const resultsTransformerUrlFragment = criteria.getTransformerQueryUrlPart();

        const urlArgs = this.urlEncodeArgs({
            query: criteria.queryText() || undefined,
            sort: criteria.sorts().filter(x => x.fieldName()).map(x => x.toQuerySortString()),
            fetch: criteria.showFields() ? "__all_fields" : undefined,
            debug: criteria.indexEntries() ? "entries" : undefined,
            operator: criteria.useAndOperator() ? "AND" : undefined,
            format: "excel",
            download: true
        }) + resultsTransformerUrlFragment;

        return url + urlArgs;
    }
}

export = queryIndexCommand;
