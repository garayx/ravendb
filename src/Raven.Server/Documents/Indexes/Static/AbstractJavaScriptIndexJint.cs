﻿using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Extensions.Jint;

namespace Raven.Server.Documents.Indexes.Static;

public abstract class AbstractJavaScriptIndexJint : AbstractJavaScriptIndex<JsHandleJint>
{
    public JintEngineEx EngineEx;
    public Engine Engine;

    protected AbstractJavaScriptIndexJint(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode, long indexVersion)
        : base(definition, configuration, mapCode, indexVersion)
    {
        // we create the engine instance directly instead of using SingleRun
        // because the index is single threaded and long lived
        var refResolver = new JintPreventResolvingTasksReferenceResolver();
        EngineEx = new JintEngineEx(configuration, refResolver);
        Engine = EngineEx.Engine;
        _engineForParsing = EngineEx.Engine;
        EngineHandle = new JintEngineEx(configuration, refResolver);
        JsUtils = JavaScriptUtilsJint.Create(null, EngineEx);
        JsIndexUtils = new JavaScriptIndexUtilsJint(JsUtils, Engine);



        lock (EngineHandle)
        {
            Initialize(modifyMappingFunctions);
        }
    }

    public override IDisposable DisableConstraintsOnInit()
    {
        return EngineHandle.DisableConstraints();
    }
    //{
    //    return new JavaScriptReduceOperation(this, JsIndexUtils, groupByKeyForParsingJint, _engineForParsing, reduce, groupByKey, _indexVersion)
    //        { ReduceString = Definition.Reduce };
    //}
    //public override ObjectInstance GetDefinitionsForParsingJint()
    //{
    //    var definitionsObj = _engineForParsing.GetValue(GlobalDefinitions);

    //    if (definitionsObj.IsNull() || definitionsObj.IsUndefined() || definitionsObj.IsObject() == false)
    //        ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

    //    var definitions = definitionsObj.AsObject();
    //    if (definitions.HasProperty(MapsProperty) == false)
    //        ThrowIndexCreationException("is missing its 'globalDefinition.maps' property, are you modifying it in your script?");

    //    return definitions;
    //}

    //protected abstract override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections,
    //    out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleJint>>>> collectionFunctions);

    public override JavaScriptReduceOperation<JsHandleJint> CreateJavaScriptReduceOperation(ScriptFunctionInstance groupByKeyForParsingJint, JsHandleJint reduce, JsHandleJint groupByKey)
    {
        return new JavaScriptReduceOperationJint(this, JsIndexUtils, groupByKeyForParsingJint, _engineForParsing, reduce, groupByKey, _indexVersion)
        { ReduceString = Definition.Reduce };
    }

    protected override List<MapMetadata> InitializeEngine(List<string> maps)
    {
        OnInitializeEngine();

        //EngineHandle.ExecuteWithReset(Code, "Code");
        //EngineHandle.ExecuteWithReset(MapCode, "MapCode");
        ////TODO: egor add those to v8
        _engineForParsing.ExecuteWithReset(Code);
        _engineForParsing.ExecuteWithReset(MapCode);

        var sb = new StringBuilder();
        if (Definition.AdditionalSources != null)
        {
            foreach (var kvpScript in Definition.AdditionalSources)
            {
                var script = kvpScript.Value;
              //  EngineHandle.ExecuteWithReset(script, $"./{Definition.Name}/additionalSource/{kvpScript.Key}");
                   _engineForParsing.ExecuteWithReset(script);
                sb.Append(Environment.NewLine);
                sb.AppendLine(script);
            }
        }

        var additionalSources = sb.ToString();
        var mapReferencedCollections = new List<MapMetadata>();
        foreach (var map in maps)
        {
           // EngineHandle.ExecuteWithReset(map, "map");
                _engineForParsing.ExecuteWithReset(map);
            var result = CollectReferencedCollections(map, additionalSources);
            mapReferencedCollections.Add(result);
        }

        if (Definition.Reduce != null)
        {
           // EngineHandle.ExecuteWithReset(Definition.Reduce, "reduce");
                _engineForParsing.ExecuteWithReset(Definition.Reduce);
        }

        return mapReferencedCollections;
    }

    public override JsHandleJint ConvertToJsHandle(object value)
    {
        switch (value)
        {
            case null:
                return EngineEx.ImplicitNull();

            case DynamicNullObject dno:
                return dno.IsExplicitNull ? EngineEx.ExplicitNull() : EngineEx.ImplicitNull();

            case DynamicBlittableJson dbj:
                var jintBoi = new BlittableObjectInstanceJint(Engine, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);
                return EngineEx.FromObjectGen(jintBoi);

            default:
                return JsUtils.TranslateToJs(context: null, value);
        }
    }

    public override JsHandleJint GetRecursiveJsFunctionInternal(JsHandleJint[] args)
    {
        var item = args[0].Item;
        var func = args[1].Item as ScriptFunctionInstance;

        if (func == null)
            throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

        var result = new RecursiveJsFunctionJint(Engine, item, func).Execute();
        return new JsHandleJint(result);
    }
}
