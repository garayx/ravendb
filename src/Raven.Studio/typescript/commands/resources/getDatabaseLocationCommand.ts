import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDatabaseLocationCommand extends commandBase {

    constructor(private inputName: string, private inputPath: string) {
        super();
    }

    execute(): JQueryPromise<Array<string>> {
        const args = {
            name: this.inputName,
            path: this.inputPath
        };

        const url =  endpoints.global.studioTasks.adminStudioTasksFullDataDirectory + this.urlEncodeArgs(args);

        return this.query<Array<string>>(url, null, null, x => x.PathDetails)
            .fail((response: JQueryXHR) => this.reportError("Failed to calculate the database full path", response.responseText, response.statusText));
    }
}

export = getDatabaseLocationCommand;
