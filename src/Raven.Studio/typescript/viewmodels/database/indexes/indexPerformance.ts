import viewModelBase = require("viewmodels/viewModelBase");
import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");
import rbush = require("rbush");
import gapFinder = require("common/helpers/graph/gapFinder");
import generalUtils = require("common/generalUtils");
import rangeAggregator = require("common/helpers/graph/rangeAggregator");
import liveIndexPerformanceWebSocketClient = require("common/liveIndexPerformanceWebSocketClient");
import inProgressAnimator = require("common/helpers/graph/inProgressAnimator");
import messagePublisher = require("common/messagePublisher");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleIndex" | "trackItem" | "gapItem";
    arg: any;
}

class hitTest {
    cursor = ko.observable<string>("auto");
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleIndex: (indexName: string) => void;
    private handleTrackTooltip: (item: Raven.Client.Documents.Indexes.IndexingPerformanceOperation, x: number, y: number) => void;   
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;
   
    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleIndex: (indeName: string) => void,
        handleTrackTooltip: (item: Raven.Client.Documents.Indexes.IndexingPerformanceOperation, x: number, y: number) => void,       
        handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
        removeTooltip: () => void) {       
        this.container = container;
        this.onToggleIndex = onToggleIndex;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;        
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Client.Documents.Indexes.IndexingPerformanceOperation) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "trackItem",
            arg: element
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerIndexToggle(x: number, y: number, width: number, height: number, indexName: string) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "toggleIndex",
            arg: indexName
        } as rTreeLeaf;
        this.rTree.insert(data);
    }
   
    registerGapItem(x: number, y: number, width: number, height: number, element: timeGapInfo) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "gapItem",
            arg: element
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    onClick() {
        const clickLocation = d3.mouse(this.container.node());

        if ((d3.event as any).defaultPrevented) {
            return;
        }

        const items = this.findItems(clickLocation[0], clickLocation[1]);

        for (let i = 0; i < items.length; i++) {
            const item = items[i];

            if (item.actionType === "toggleIndex") {
                 this.onToggleIndex(item.arg as string);
            } 
        }
    }

    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const overToggleIndex = items.filter(x => x.actionType === "toggleIndex").length > 0;
        this.cursor(overToggleIndex ? "pointer" : "auto");

        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Client.Documents.Indexes.IndexingPerformanceOperation)[0];
        if (currentItem) {
            this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
        }
        else {
            const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as timeGapInfo)[0];
            if (currentGapItem) {
                this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
            }
            else {
                this.removeTooltip();
            }
        }        
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - indexPerformance.brushSectionHeight,
            maxY: y - indexPerformance.brushSectionHeight
        });
    }
}

class indexPerformance extends viewModelBase {

    /* static */

    static readonly colors = {
        axis: "#546175",
        gaps: "#ca1c59",
        brushChartColor: "#37404b",
        brushChartStrokeColor: "#008cc9",
        trackBackground: "#2c343a",
        trackNameBg: "rgba(57, 67, 79, 0.8)",
        trackNameFg: "#98a7b7",
        openedTrackArrow: "#ca1c59",
        closedTrackArrow: "#98a7b7",
        collectionNameTextColor: "#2c343a",

        tracks: {
            "Collection": "#046293",
            "Indexing": "#607d8b",
            "Cleanup": "#1a858e",
            "References": "#ac2258",
            "Map": "#0b4971",
            "Storage/DocumentRead": "#0077b5",
            "Linq": "#008cc9",
            "LoadDocument": "#008cc9",
            "Bloom": "#34b3e4",
            "Lucene/Delete": "#66418c",
            "Lucene/AddDocument": "#8d6cab",
            "Lucene/Convert": "#7b539d",
            "CreateBlittableJson": "#313fa0",
            "Aggregation/BlittableJson": "#ec407a",
            "GetMapEntriesTree": "#689f39",
            "GetMapEntries": "#8cc34b",
            "Storage/RemoveMapResult": "#ff7000",
            "Storage/PutMapResult": "#fe8f01",
            "Reduce": "#98041b",
            "Tree": "#af1923",
            "Aggregation/Leafs": "#890e4f",
            "Aggregation/Branches": "#ad1457",
            "Storage/ReduceResults": "#e65100",
            "NestedValues": "#795549",
            "Storage/Read": "#faa926",
            "Aggregation/NestedValues": "#d81a60",
            "Lucene/FlushToDisk": "#a487ba",
            "Storage/Commit": "#5b912d",
            "Lucene/RecreateSearcher": "#b79ec7",
            "SaveOutputDocuments": "#fed101"
        }
    }

    static readonly brushSectionHeight = 40;
    private static readonly brushSectionIndexesWorkHeight = 22;
    private static readonly brushSectionLineWidth = 1;
    private static readonly trackHeight = 18; // height used for callstack item
    private static readonly stackPadding = 1; // space between call stacks
    private static readonly trackMargin = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly openedTrackPadding = 4;
    private static readonly axisHeight = 35; 
    private static readonly inProgressStripesPadding = 7;

    private static readonly maxRecursion = 5;
    private static readonly minGapSize = 10 * 1000; // 10 seconds
    private static readonly initialOffset = 100;
    private static readonly step = 200;


    private static readonly openedTrackHeight = indexPerformance.openedTrackPadding
    + (indexPerformance.maxRecursion + 1) * indexPerformance.trackHeight
    + indexPerformance.maxRecursion * indexPerformance.stackPadding
    + indexPerformance.openedTrackPadding;

    private static readonly closedTrackHeight = indexPerformance.closedTrackPadding
    + indexPerformance.trackHeight
    + indexPerformance.closedTrackPadding;

    /* observables */

    hasAnyData = ko.observable<boolean>(false);
    private searchText = ko.observable<string>();

    private liveViewClient = ko.observable<liveIndexPerformanceWebSocketClient>();
    private autoScroll = ko.observable<boolean>(false);

    private indexNames = ko.observableArray<string>();
    private filteredIndexNames = ko.observableArray<string>();
    private expandedTracks = ko.observableArray<string>();
    private isImport = ko.observable<boolean>(false);
    private importFileName = ko.observable<string>();

    private canExpandAll: KnockoutComputed<boolean>;

    /* private */

    private data: Raven.Client.Documents.Indexes.IndexPerformanceStats[] = [];
    private totalWidth: number;
    private totalHeight: number;
    private currentYOffset = 0;
    private maxYOffset = 0;
    private hitTest = new hitTest();
    private gapFinder: gapFinder;
    private dialogVisible = false;

    private inProgressAnimator: inProgressAnimator;
    private inProgressMarkerCanvas: HTMLCanvasElement;    

    /* d3 */

    private xTickFormat = d3.time.format("%H:%M:%S");
    private canvas: d3.Selection<any>;
    private inProgressCanvas: d3.Selection<any>;
    private svg: d3.Selection<any>; // spans to canvas size (to provide brush + zoom/pan features)
    private brush: d3.svg.Brush<number>;
    private brushAndZoomCallbacksDisabled = false;
    private xBrushNumericScale: d3.scale.Linear<number, number>;
    private xBrushTimeScale: d3.time.Scale<number, number>;
    private yBrushValueScale: d3.scale.Linear<number, number>;
    private xNumericScale: d3.scale.Linear<number, number>;
    private brushSection: HTMLCanvasElement; // virtual canvas for brush section
    private brushContainer: d3.Selection<any>;
    private zoom: d3.behavior.Zoom<any>;
    private yScale: d3.scale.Ordinal<string, number>;
    private tooltip: d3.Selection<Raven.Client.Documents.Indexes.IndexingPerformanceOperation | timeGapInfo>;      

    constructor() {
        super();

        this.canExpandAll = ko.pureComputed(() => {
            const indexNames = this.indexNames();
            const expandedTracks = this.expandedTracks();

            return indexNames.length && indexNames.length !== expandedTracks.length;
        });

        this.searchText.throttle(200).subscribe(() => {
            this.filterIndexes();
            this.drawMainSection();
        });

        this.autoScroll.subscribe(v => {
            if (v) {
                this.scrollToRight();
            } else {
                // cancel transition (if any)
                this.brushContainer
                    .transition(); 
            }
        });
    }

    activate(args: { indexName: string, database: string}): void {
        super.activate(args);

        if (args.indexName) {
            this.expandedTracks.push(args.indexName);
        }
    }

    deactivate() {
        super.deactivate();

        if (this.liveViewClient()) {
            this.cancelLiveView();
        }
    }

    compositionComplete() {
        super.compositionComplete();

        this.tooltip = d3.select(".tooltip");

        [this.totalWidth, this.totalHeight] = this.getPageHostDimenensions();
        this.totalWidth -= 1;

        this.totalHeight -= 50; // substract toolbar height

        this.initCanvases();

        this.hitTest.init(this.svg,
            (indexName) => this.onToggleIndex(indexName),          
            (item, x, y) => this.handleTrackTooltip(item, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());

        this.enableLiveView();
    }

    private initCanvases() {
        const metricsContainer = d3.select("#indexPerfMetricsContainer");
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight);

        this.inProgressCanvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight - indexPerformance.brushSectionHeight)
            .style("top", (indexPerformance.brushSectionHeight + indexPerformance.axisHeight) + "px");

        const inProgressCanvasNode = this.inProgressCanvas.node() as HTMLCanvasElement;
        const inProgressContext = inProgressCanvasNode.getContext("2d");
        inProgressContext.translate(0, -indexPerformance.axisHeight);

        this.inProgressAnimator = new inProgressAnimator(inProgressCanvasNode);

        this.svg = metricsContainer
            .append("svg")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight);

        this.xBrushNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth])
            .domain([0, this.totalWidth]);

        this.xNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth])
            .domain([0, this.totalWidth]);

        this.brush = d3.svg.brush()
            .x(this.xBrushNumericScale)
            .on("brush", () => this.onBrush());

        this.zoom = d3.behavior.zoom()
            .x(this.xNumericScale)
            .on("zoom", () => this.onZoom());

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight - indexPerformance.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + indexPerformance.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
        let mousePressed = false;

        const onMove = () => {
            this.hitTest.onMouseMove();
        }

        this.hitTest.cursor.subscribe((cursor) => {
            selection.style("cursor", cursor);
        });

        selection.on("mousemove.tip", onMove);

        selection.on("click", () => this.hitTest.onClick());

        selection
            .on("mousedown.tip", () => selection.on("mousemove.tip", null))
            .on("mouseup.tip", () => selection.on("mousemove.tip", onMove));

        selection
            .on("mousedown.live", () => {
                if (this.liveViewClient()) {
                    this.liveViewClient().pauseUpdates();
                }
            });
        selection
            .on("mouseup.live", () => {
                if (this.liveViewClient()) {
                    this.liveViewClient().resumeUpdates();
                }
            });

        selection
            .on("mousedown.yShift", () => {
                const node = selection.node();
                const initialClickLocation = d3.mouse(node);
                const initialOffset = this.currentYOffset;

                selection.on("mousemove.yShift", () => {
                    const currentMouseLocation = d3.mouse(node);
                    const yDiff = currentMouseLocation[1] - initialClickLocation[1];

                    const newYOffset = initialOffset - yDiff;

                    this.currentYOffset = newYOffset;
                    this.fixCurrentOffset();
                });

                selection.on("mouseup.yShift", () => selection.on("mousemove.yShift", null));
            });

        selection.on("dblclick.zoom", null);
    }

    private filterIndexes() {
        const criteria = this.searchText().toLowerCase();

        this.filteredIndexNames(this.indexNames().filter(x => x.toLowerCase().includes(criteria)));
    }

    private enableLiveView() {
        let firstTime = true;

        const onDataUpdate = (data: Raven.Client.Documents.Indexes.IndexPerformanceStats[]) => {
            let timeRange: [Date, Date];
            if (!firstTime) {
                const timeToRemap = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
                timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x));
            }

            this.data = data;

            const [workData, maxConcurrentIndexes] = this.prepareTimeData();

            if (!firstTime) {
                const newBrush: [number, number] = timeRange.map(x => this.xBrushTimeScale(x));
                this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
            }

            if (this.autoScroll()) {
                this.scrollToRight();
            }

            this.draw(workData, maxConcurrentIndexes, firstTime);

            if (firstTime) {
                firstTime = false;
            }
        };

        this.liveViewClient(new liveIndexPerformanceWebSocketClient(this.activeDatabase(), onDataUpdate));
    }

    scrollToRight() {
        const currentExtent = this.brush.extent() as [number, number];
        const extentWidth = currentExtent[1] - currentExtent[0];

        const existingBrushStart = currentExtent[0];

        if (currentExtent[1] < this.totalWidth) {

            const rightPadding = 100;
            const desiredShift = rightPadding * extentWidth / this.totalWidth;

            const desiredExtentStart = this.totalWidth + desiredShift - extentWidth;

            const moveFunc = (startX: number) => {
                this.brush.extent([startX, startX + extentWidth]);
                this.brushContainer.call(this.brush);

                this.onBrush();
            };

            this.brushContainer
                .transition()
                .duration(500)
                .tween("side-effect", () => {
                    const interpolator = d3.interpolate(existingBrushStart, desiredExtentStart);

                    return (t) => {
                        const currentStart = interpolator(t);
                        moveFunc(currentStart);
                    }
                });
        }
    }

    private cancelLiveView() {
        if (!!this.liveViewClient()) {
            this.liveViewClient().dispose();
            this.liveViewClient(null);
        }
    }

    private draw(workData: indexesWorkData[], maxConcurrentIndexes: number, resetFilteredIndexNames: boolean) {
        this.hasAnyData(this.data.length > 0);

        this.prepareBrushSection(workData, maxConcurrentIndexes);
        this.prepareMainSection(resetFilteredIndexNames);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.clearRect(0, 0, this.totalWidth, indexPerformance.brushSectionHeight);
        context.drawImage(this.brushSection, 0, 0);
        this.drawMainSection();
    }

    private prepareTimeData(): [indexesWorkData[], number] {
        let timeRanges = this.extractTimeRanges(); 

        let maxConcurrentIndexes: number;
        let workData: indexesWorkData[];

        if (timeRanges.length === 0) {
            // no data - create fake scale
            timeRanges = [[new Date(), new Date()]];
            maxConcurrentIndexes = 1;
            workData = [];
        } else {
            const aggregatedRanges = new rangeAggregator(timeRanges);
            workData = aggregatedRanges.aggregate();
            maxConcurrentIndexes = aggregatedRanges.maxConcurrentIndexes;
        }

        this.gapFinder = new gapFinder(timeRanges, indexPerformance.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        return [workData, maxConcurrentIndexes];
    }

    private prepareBrushSection(workData: indexesWorkData[], maxConcurrentIndexes: number) {
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth + 1;
        this.brushSection.height = indexPerformance.brushSectionHeight;

        this.yBrushValueScale = d3.scale.linear()
            .domain([0, maxConcurrentIndexes])
            .range([0, indexPerformance.brushSectionIndexesWorkHeight]); 

        const context = this.brushSection.getContext("2d");

        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, indexPerformance.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = indexPerformance.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth, indexPerformance.brushSectionHeight - 1);

        context.fillStyle = indexPerformance.colors.brushChartColor;  
        context.strokeStyle = indexPerformance.colors.brushChartStrokeColor; 
        context.lineWidth = indexPerformance.brushSectionLineWidth;

        // Draw area chart showing indexes work
        let x1: number, x2: number, y0: number = 0, y1: number;
        for (let i = 0; i < workData.length - 1; i++) {

            context.beginPath();
            x1 = this.xBrushTimeScale(new Date(workData[i].pointInTime));
            y1 = Math.round(this.yBrushValueScale(workData[i].numberOfIndexesWorking)) + 0.5;
            x2 = this.xBrushTimeScale(new Date(workData[i + 1].pointInTime));
            context.moveTo(x1, indexPerformance.brushSectionHeight - y0);
            context.lineTo(x1, indexPerformance.brushSectionHeight - y1);

            // Don't want to draw line -or- rect at level 0
            if (y1 !== 0) {
                context.lineTo(x2, indexPerformance.brushSectionHeight - y1);
                context.fillRect(x1, indexPerformance.brushSectionHeight - y1, x2-x1, y1);
            } 

            context.stroke();
            y0 = y1; 
        }

        // Draw last line:
        context.beginPath();
        context.moveTo(x2, indexPerformance.brushSectionHeight - y1);
        context.lineTo(x2, indexPerformance.brushSectionHeight);
        context.stroke(); 

        this.drawBrushGaps(context);
        this.prepareBrush();
    }

    private drawBrushGaps(context: CanvasRenderingContext2D) {
        for (let i = 0; i < this.gapFinder.gapsPositions.length; i++) {
            const gap = this.gapFinder.gapsPositions[i];

            context.strokeStyle = indexPerformance.colors.gaps;

            const gapX = this.xBrushTimeScale(gap.start);
            context.moveTo(gapX, 1);
            context.lineTo(gapX, indexPerformance.brushSectionHeight - 2);
            context.stroke();
        }
    }

    private prepareBrush() {
        const hasBrush = !!this.svg.select("g.brush").node();

        if (!hasBrush) {
            this.brushContainer = this.svg
                .append("g")
                .attr("class", "x brush");

            this.brushContainer
                .call(this.brush)
                .selectAll("rect")
                .attr("y", 0)
                .attr("height", indexPerformance.brushSectionHeight - 1);
        }
    }

    private prepareMainSection(resetFilteredIndexNames: boolean) {
        this.findAndSetIndexNames();

        if (resetFilteredIndexNames) {
            this.searchText("");
        }
        this.filterIndexes();
    }

    private findAndSetIndexNames() {
        this.indexNames(_.uniq(this.data.map(x => x.IndexName)));
    }

    private fixCurrentOffset() {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale() {
        let currentOffset = indexPerformance.axisHeight - this.currentYOffset;
        let domain = [] as Array<string>;
        let range = [] as Array<number>;

        const indexesInfo = this.filteredIndexNames();

        for (let i = 0; i < indexesInfo.length; i++) {
            const indexName = indexesInfo[i];

            domain.push(indexName);
            range.push(currentOffset);

            const isOpened = _.includes(this.expandedTracks(), indexName);

            const itemHeight = isOpened ? indexPerformance.openedTrackHeight : indexPerformance.closedTrackHeight;

            currentOffset += itemHeight + indexPerformance.trackMargin;
        }

        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset() {
        const expandedTracksCount = this.expandedTracks().length;
        const closedTracksCount = this.filteredIndexNames().length - expandedTracksCount;

        const offset = indexPerformance.axisHeight
            + this.filteredIndexNames().length * indexPerformance.trackMargin
            + expandedTracksCount * indexPerformance.openedTrackHeight
            + closedTracksCount * indexPerformance.closedTrackHeight;

        const availableHeightForTracks = this.totalHeight - indexPerformance.brushSectionHeight;

        const extraBottomMargin = 100;

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private getTicks(scale: d3.time.Scale<number, number>) : Date[] {
        return d3.range(indexPerformance.initialOffset, this.totalWidth - indexPerformance.step, indexPerformance.step)
            .map(y => scale.invert(y));
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number) {
        try {
            context.save();
            context.beginPath();

            context.setLineDash([4, 2]);
            context.strokeStyle = indexPerformance.colors.axis;
           
            ticks.forEach((x, i) => {
                context.moveTo(indexPerformance.initialOffset + (i * indexPerformance.step) + 0.5, yStart);
                context.lineTo(indexPerformance.initialOffset + (i * indexPerformance.step) + 0.5, yEnd);
            });

            context.stroke();
        }
        finally {
            context.restore();
        }
    }

    private drawXaxisTimeLabels(context: CanvasRenderingContext2D, ticks: Date[], timePaddingLeft: number, timePaddingTop: number) {
        try {
            context.save();
            context.beginPath();

            context.textAlign = "left";
            context.textBaseline = "top";
            context.font = "10px Lato";
            context.fillStyle = indexPerformance.colors.axis;
           
            ticks.forEach((x, i) => {
                context.fillText(this.xTickFormat(x), indexPerformance.initialOffset + (i * indexPerformance.step) + timePaddingLeft, timePaddingTop);
            });            
        }
        finally {
            context.restore();
        }
    }   

    private onZoom() {
        this.autoScroll(false);
        if (!this.brushAndZoomCallbacksDisabled) {
            this.brush.extent(this.xNumericScale.domain() as [number, number]);
            this.brushContainer
                .call(this.brush);

            this.drawMainSection();
        }
    }

    private onBrush() {
        if (!this.brushAndZoomCallbacksDisabled) {
            this.xNumericScale.domain((this.brush.empty() ? this.xBrushNumericScale.domain() : this.brush.extent()) as [number, number]);
            this.zoom.x(this.xNumericScale);
            this.drawMainSection();
        }
    }

    private extractTimeRanges(): Array<[Date, Date]> {
        const result = [] as Array<[Date, Date]>;
        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                const perfStatsWithCache = perfStat as IndexingPerformanceStatsWithCache;
                const start = perfStatsWithCache.StartedAsDate;
                let end: Date;
                if (perfStat.Completed) {
                    end = perfStatsWithCache.CompletedAsDate;
                } else {
                    end = new Date(start.getTime() + perfStat.DurationInMilliseconds);
                }
                result.push([start, end]);
            });
        });

        return result;
    }

    private drawMainSection() {
        this.inProgressAnimator.reset();
        this.hitTest.reset();
        this.calcMaxYOffset();
        this.fixCurrentOffset();
        this.constructYScale();

        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];

        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.save();
        try {
            context.translate(0, indexPerformance.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);

            this.drawTracksBackground(context, xScale);

            if (xScale.domain().length) {               
                const ticks = this.getTicks(xScale);

                context.save();
                context.beginPath();
                context.rect(0, indexPerformance.axisHeight - 3, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);
                context.clip();
                const timeYStart = this.yScale.range()[0] || indexPerformance.axisHeight;
                this.drawXaxisTimeLines(context, ticks, timeYStart - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }

            context.save();
            try {
                context.beginPath();
                context.rect(0, indexPerformance.axisHeight, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);
                context.clip();

                this.drawTracks(context, xScale, visibleTimeFrame);
                this.drawIndexNames(context);
                this.drawGaps(context, xScale);
            } finally {
                context.restore();
            }
        } finally {
            context.restore();
        }

        this.inProgressAnimator.animate();
    }

    private drawTracksBackground(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        context.save();

        context.beginPath();
        context.rect(0, indexPerformance.axisHeight, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);
        context.clip();

        this.data.forEach(perfStat => {
            const yStart = this.yScale(perfStat.IndexName);

            const isOpened = _.includes(this.expandedTracks(), perfStat.IndexName);

            context.beginPath();
            context.fillStyle = indexPerformance.colors.trackBackground;
            context.fillRect(0, yStart, this.totalWidth, isOpened ? indexPerformance.openedTrackHeight : indexPerformance.closedTrackHeight);
        });

        context.restore();
    }

    private drawTracks(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>, visibleTimeFrame: [Date, Date]) {
        if (xScale.domain().length === 0) {
            return;
        }

        const visibleStartDateAsInt = visibleTimeFrame[0].getTime();
        const visibleEndDateAsInt = visibleTimeFrame[1].getTime();

        const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(xScale);

        this.data.forEach(perfStat => {
            if (!_.includes(this.filteredIndexNames(), perfStat.IndexName)) {
                return;
            }

            const isOpened = _.includes(this.expandedTracks(), perfStat.IndexName);
            let yStart = this.yScale(perfStat.IndexName);
            yStart += isOpened ? indexPerformance.openedTrackPadding : indexPerformance.closedTrackPadding;

            const performance = perfStat.Performance;
            const perfLength = performance.length;
            for (let perfIdx = 0; perfIdx < perfLength; perfIdx++) {
                const perf = performance[perfIdx];
                const startDate = (perf as IndexingPerformanceStatsWithCache).StartedAsDate;
                const x1 = xScale(startDate);

                const startDateAsInt = startDate.getTime();

                const endDateAsInt = startDateAsInt + perf.DurationInMilliseconds;
                if (endDateAsInt < visibleStartDateAsInt || visibleEndDateAsInt < startDateAsInt)
                    continue;

                const yOffset = isOpened ? indexPerformance.trackHeight + indexPerformance.stackPadding : 0;
                this.drawStripes(context, [perf.Details], x1, yStart + (isOpened ? yOffset : 0), yOffset, extentFunc);

                if (!perf.Completed) {
                    this.findInProgressAction(context, perf, extentFunc, x1, yStart + (isOpened ? yOffset : 0), yOffset);
                }
            }
        });
    }

    private findInProgressAction(context: CanvasRenderingContext2D, perf: Raven.Client.Documents.Indexes.IndexingPerformanceStats, extentFunc: (duration: number) => number,
        xStart: number, yStart: number, yOffset: number): void {

        const extractor = (perfs: Raven.Client.Documents.Indexes.IndexingPerformanceOperation[], xStart: number, yStart: number, yOffset: number) => {

            let currentX = xStart;

            perfs.forEach(op => {
                const dx = extentFunc(op.DurationInMilliseconds);

                this.inProgressAnimator.register([currentX, yStart, dx, indexPerformance.trackHeight]);

                if (op.Operations.length > 0) {
                    extractor(op.Operations, currentX, yStart + yOffset, yOffset);
                }
                currentX += dx;
            });
        }

        extractor([perf.Details], xStart, yStart, yOffset);
    }

    private getColorForOperation(operationName: string): string {
        const { tracks } = indexPerformance.colors;
        if (operationName in tracks) {
            return (tracks as dictionary<string>)[operationName];
        }

        if (operationName.startsWith("Collection_")) {
            return tracks.Collection;
        }

        throw new Error("Unable to find color for: " + operationName);
    }

    private drawStripes(context: CanvasRenderingContext2D, operations: Array<Raven.Client.Documents.Indexes.IndexingPerformanceOperation>, xStart: number, yStart: number,
        yOffset: number, extentFunc: (duration: number) => number) {

        let currentX = xStart;
        const length = operations.length;
        for (let i = 0; i < length; i++) {
            const op = operations[i];
            context.fillStyle = this.getColorForOperation(op.Name);

            const dx = extentFunc(op.DurationInMilliseconds);

            context.fillRect(currentX, yStart, dx, indexPerformance.trackHeight);

            if (yOffset !== 0) { // track is opened
                if (dx >= 0.8) { // don't show tooltip for very small items
                    this.hitTest.registerTrackItem(currentX, yStart, dx, indexPerformance.trackHeight, op);
                }
                if (op.Name.startsWith("Collection_")) {
                    context.fillStyle = indexPerformance.colors.collectionNameTextColor;
                    const text = op.Name.substr("Collection_".length);
                    const textWidth = context.measureText(text).width
                    const truncatedText = graphHelper.truncText(text, textWidth, dx - 4);
                    if (truncatedText) {
                        context.font = "12px Lato";
                        context.fillText(truncatedText, currentX + 2, yStart + 13, dx - 4);
                    }
                }
            }
            
            if (op.Operations.length > 0) {
                this.drawStripes(context, op.Operations, currentX, yStart + yOffset, yOffset, extentFunc);
            }
            currentX += dx;
        }
    }

    private drawIndexNames(context: CanvasRenderingContext2D) {
        const yScale = this.yScale;
        const textShift = 14.5;
        const textStart = 3 + 8 + 4;

        this.filteredIndexNames().forEach((indexName) => {
            context.font = "12px Lato";
            const rectWidth = context.measureText(indexName).width + 2 * 3 /* left right padding */ + 8 /* arrow space */ + 4; /* padding between arrow and text */ 

            context.fillStyle = indexPerformance.colors.trackNameBg;
            context.fillRect(2, yScale(indexName) + indexPerformance.closedTrackPadding, rectWidth, indexPerformance.trackHeight);
            this.hitTest.registerIndexToggle(2, yScale(indexName), rectWidth, indexPerformance.trackHeight, indexName);
            context.fillStyle = indexPerformance.colors.trackNameFg;
            context.fillText(indexName, textStart + 0.5, yScale(indexName) + textShift);

            const isOpened = _.includes(this.expandedTracks(), indexName);
            context.fillStyle = isOpened ? indexPerformance.colors.openedTrackArrow : indexPerformance.colors.closedTrackArrow;
            graphHelper.drawArrow(context, 5, yScale(indexName) + 6, !isOpened);
        });
    }

    private drawGaps(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {      
        // xScale.range has screen pixels locations of Activity periods
        // xScale.domain has Start & End times of Activity periods

        const range = xScale.range();

        context.beginPath();
        context.strokeStyle = indexPerformance.colors.gaps;       

        for (let i = 1; i < range.length - 1; i += 2) { 
            const gapX = Math.floor(range[i]) + 0.5;
            
            context.moveTo(gapX, indexPerformance.axisHeight);
            context.lineTo(gapX, this.totalHeight);

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = xScale.domain()[i];
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                this.hitTest.registerGapItem(gapX - 5, indexPerformance.axisHeight, 10, this.totalHeight,
                    { durationInMillis: gapInfo.durationInMillis, start: gapInfo.start });
            }
        }

        context.stroke();
    }

    private onToggleIndex(indexName: string) {
        if (_.includes(this.expandedTracks(), indexName)) {
            this.expandedTracks.remove(indexName);
        } else {
            this.expandedTracks.push(indexName);
        }

        this.drawMainSection();
    }

    expandAll() {
        this.expandedTracks(this.indexNames().slice());
        this.drawMainSection();
    }

    collapseAll() {
        this.expandedTracks([]);
        this.drawMainSection();
    }
   
    private handleGapTooltip(element: timeGapInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            const tooltipHtml = "Gap start time: " + (element).start.toLocaleTimeString() +
                "<br/>Gap duration: " + generalUtils.formatMillis((element).durationInMillis);       
            this.handleTooltip(element, x, y, tooltipHtml);
        }
    } 

    private handleTrackTooltip(element: Raven.Client.Documents.Indexes.IndexingPerformanceOperation, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            let tooltipHtml = `${element.Name}<br/>Duration: ${generalUtils.formatMillis((element).DurationInMilliseconds)}`;

            const opWithParent = element as IndexingPerformanceOperationWithParent;

            if (opWithParent.Parent) {
                const parentStats = opWithParent.Parent;
                let countsDetails: string;
                countsDetails = `<br/>*** Entries details ***<br/>`;
                countsDetails += `Input Count: ${parentStats.InputCount.toLocaleString()}<br/>`;
                countsDetails += `Output Count: ${parentStats.OutputCount.toLocaleString()}<br/>`;
                countsDetails += `Failed Count: ${parentStats.FailedCount.toLocaleString()}<br/>`;
                countsDetails += `Success Count: ${parentStats.SuccessCount.toLocaleString()}<br/>`;

                tooltipHtml += countsDetails;
            }

            if (element.CommitDetails) {   
                let commitDetails: string;
                commitDetails = `<br/>*** Commit details ***<br/>`;
                commitDetails += `Modified pages: ${element.CommitDetails.NumberOfModifiedPages.toLocaleString()}<br/>`;
                commitDetails += `Pages written to disk: ${element.CommitDetails.NumberOf4KbsWrittenToDisk.toLocaleString()}`;
                tooltipHtml += commitDetails;
            }
            if (element.MapDetails) {
                let mapDetails: string;
                mapDetails = `<br/>*** Map details ***<br/>`;
                mapDetails += `Allocation budget: ${generalUtils.formatBytesToSize(element.MapDetails.AllocationBudget)}<br/>`;
                mapDetails += `Batch status: ${element.MapDetails.BatchCompleteReason || 'In progress'}<br/>`;
                mapDetails += `Currently allocated: ${generalUtils.formatBytesToSize(element.MapDetails.CurrentlyAllocated)} <br/>`;
                mapDetails += `Process private memory: ${generalUtils.formatBytesToSize(element.MapDetails.ProcessPrivateMemory)}<br/>`;
                mapDetails += `Process working set: ${generalUtils.formatBytesToSize(element.MapDetails.ProcessWorkingSet)}`;
                tooltipHtml += mapDetails;
            }
            if (element.ReduceDetails) {
                let reduceDetails: string;
                reduceDetails = `<br/>*** Reduce details ***<br/>`;
                reduceDetails += `Compressed leaves: ${element.ReduceDetails.NumberOfCompressedLeafs.toLocaleString()}<br/>`;
                reduceDetails += `Modified branches: ${element.ReduceDetails.NumberOfModifiedBranches.toLocaleString()}<br/>`;
                reduceDetails += `Modified leaves: ${element.ReduceDetails.NumberOfModifiedLeafs.toLocaleString()}`;
                tooltipHtml += reduceDetails;
            }           

            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }
    
    private handleTooltip(element: Raven.Client.Documents.Indexes.IndexingPerformanceOperation | timeGapInfo, x: number, y: number, tooltipHtml: string) {
        if (element && !this.dialogVisible) {
            const canvas = this.canvas.node() as HTMLCanvasElement;
            const context = canvas.getContext("2d");
            context.font = this.tooltip.style("font");

            const longestLine = generalUtils.findLongestLine(tooltipHtml); 
            const tooltipWidth = context.measureText(longestLine).width + 60;

            const numberOfLines = generalUtils.findNumberOfLines(tooltipHtml);
            const tooltipHeight = numberOfLines * 30 + 60;

            x = Math.min(x, Math.max(this.totalWidth - tooltipWidth, 0));
            y = Math.min(y, Math.max(this.totalHeight - tooltipHeight, 0));

            this.tooltip
                .style("left", (x + 10) + "px")
                .style("top", (y + 10) + "px");

            this.tooltip
                .transition()
                .duration(250)
                .style("opacity", 1);

            this.tooltip
                .html(tooltipHtml)
                .datum(element);
        } else {
            this.hideTooltip();
        }
    }    

    private hideTooltip() {
        this.tooltip.transition()
            .duration(250)
            .style("opacity", 0);
         
        this.tooltip.datum(null);      
    }

    fileSelected() { 
        const fileInput = <HTMLInputElement>document.querySelector("#importFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function() {
// ReSharper disable once SuspiciousThisUsage
            self.dataImported(this.result);
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsText(file);

        this.importFileName(fileInput.files[0].name);

        // Must clear the filePicker element value so that user will be able to import the -same- file after closing the imported view...
        const $input = $("#importFilePicker");
        $input.val(null);
    }

    private dataImported(result: string) {
        this.cancelLiveView();

        try {            
            const importedData: Raven.Client.Documents.Indexes.IndexPerformanceStats[] = JSON.parse(result);

            // Data validation
            if (!_.isArray(importedData)) {
                messagePublisher.reportError("Invalid indexing performance file format", undefined, undefined);
            }
            else {                                
                this.data = importedData;
                this.fillCache();
                this.resetGraphData();
                const [workData, maxConcurrentIndexes] = this.prepareTimeData();
                this.draw(workData, maxConcurrentIndexes, true);
                this.isImport(true);
            }         
        }
        catch (e) {
            messagePublisher.reportError("Failed to parse json data", undefined, undefined);
        }              
    }

    private fillCache() {
        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                liveIndexPerformanceWebSocketClient.fillCache(perfStat);
            });
        });
    }

    closeImport() {
        this.isImport(false);
        this.resetGraphData();
        this.enableLiveView();
    }

    private resetGraphData() {
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());

        this.expandedTracks([]);
        this.searchText("");
    }

    private setZoomAndBrush(scale: [number, number], brushAction: (brush: d3.svg.Brush<any>) => void) {
        this.brushAndZoomCallbacksDisabled = true;

        this.xNumericScale.domain(scale);
        this.zoom.x(this.xNumericScale);

        brushAction(this.brush);
        this.brushContainer.call(this.brush);

        this.brushAndZoomCallbacksDisabled = false;
    }

    exportAsJson() {  
        let exportFileName;

        if (this.isImport()) {           
            exportFileName = this.importFileName().substring(0, this.importFileName().lastIndexOf('.'));                    
        } else {
            exportFileName = `indexPerf of ${this.activeDatabase().name} ${moment().format("YYYY-MM-DD HH-mm")}`; 
        }

        const keysToIgnore: Array<keyof IndexingPerformanceStatsWithCache | keyof IndexingPerformanceOperationWithParent> = ["StartedAsDate", "CompletedAsDate", "Parent"];
        fileDownloader.downloadAsJson(this.data, exportFileName + ".json", exportFileName, (key, value) => {
            if (_.includes(keysToIgnore, key)) {
                return undefined;
            }
            return value;
        });
    }

}

export = indexPerformance; 
 
