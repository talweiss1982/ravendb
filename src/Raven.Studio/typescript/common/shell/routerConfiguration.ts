﻿import appUrl = require("common/appUrl");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

import getManageServerMenuItem = require("common/shell/menu/items/manageServer");
import getDatabasesMenuItem = require("common/shell/menu/items/databases");
import getStatsMenuItem = require("common/shell/menu/items/stats");
import getSettingsMenuItem = require("common/shell/menu/items/settings");
import getTasksMenuItem = require("common/shell/menu/items/tasks");
import getIndexesMenuItem = require("common/shell/menu/items/indexes");
import getTransformersMenuItems = require("common/shell/menu/items/transformers");
import getDocumentsMenuItem = require("common/shell/menu/items/documents");

export = getRouterConfiguration();

function getRouterConfiguration(): Array<DurandalRouteConfiguration> {
    return generateAllMenuItems()
        .map(getMenuItemDurandalRoutes)
        .reduce((result, next) => result.concat(next), [])
        .reduce((result: any[], next: any) => {
            let nextJson = JSON.stringify(next);
            if (!result.some(x => JSON.stringify(x) === nextJson)) {
                result.push(next);
            }

            return result;
        }, []) as Array<DurandalRouteConfiguration>;
}


function convertToDurandalRoute(leaf: leafMenuItem): DurandalRouteConfiguration {
    return {
        route: leaf.route,
        title: leaf.title,
        moduleId: leaf.moduleId,
        nav: leaf.nav,
        dynamicHash: leaf.dynamicHash
    };
}


function getMenuItemDurandalRoutes(item: menuItem): Array<DurandalRouteConfiguration> {
    if (item.type === 'intermediate') {
        var intermediateItem = item as intermediateMenuItem;
        return intermediateItem.children
            .map(child => getMenuItemDurandalRoutes(child))
            .reduce((result, next) => result.concat(next), []);
    } else if (item.type === 'leaf') {
        return [convertToDurandalRoute(item as leafMenuItem)];
    }

    return [];
}


function generateAllMenuItems() {
    let appUrls = appUrl.forCurrentDatabase();
    return [
        getDocumentsMenuItem(appUrls),
        getIndexesMenuItem(appUrls),
        ...getTransformersMenuItems(appUrls),
        getTasksMenuItem(appUrls),
        getSettingsMenuItem(appUrls),
        getStatsMenuItem(appUrls),
        getDatabasesMenuItem(appUrls),
        getManageServerMenuItem(),
        new leafMenuItem({
            route: 'about',
            moduleId: 'viewmodels/shell/about',
            title: 'About',
            tooltip: "About",
            nav: true,
            css: 'icon-info',
            dynamicHash: appUrl.forAbout
        })
    ];
}



