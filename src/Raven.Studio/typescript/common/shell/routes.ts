﻿
import MENU_BASED_ROUTER_CONFIGURATION = require("common/shell/routerConfiguration");

export = Routes;

class Routes {

    static get(appUrls: computedAppUrls): Array<DurandalRouteConfiguration> {
        let routes = [
            {
                route: "databases/upgrade",
                title: "Upgrade in progress",
                moduleId: "viewmodels/common/upgrade",
                nav: false,
                dynamicHash: appUrls.upgrade
            },
            {
                route: "databases/edit",
                title: "Edit Document",
                moduleId: "viewmodels/database/documents/editDocument",
                nav: false
            }
        ] as Array<DurandalRouteConfiguration>;

        return routes.concat(MENU_BASED_ROUTER_CONFIGURATION);
    }

}

