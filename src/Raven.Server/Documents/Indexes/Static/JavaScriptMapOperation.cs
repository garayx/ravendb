using System;
using System.Collections.Generic;
using System.Linq;
using Esprima.Ast;
using Jint;
using Jint.Native.Array;
using Jint.Native;
using Jint.Native.Function;
using Jint.Runtime.Environments;
using Jint.Runtime;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.Jint;
using JavaScriptException = Jint.Runtime.JavaScriptException;
using JintPreventResolvingTasksReferenceResolver = Raven.Server.Documents.Patch.Jint.JintPreventResolvingTasksReferenceResolver;
using V8Exception = V8.Net.V8Exception;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptMapOperationV8 : JavaScriptMapOperation<JsHandleV8>
    {
        private V8EngineEx EngineExV8 => _engineHandle as V8EngineEx;
        private readonly AbstractJavaScriptIndexV8 IndexV8;
        protected override JsHandleV8 CreateFunctionObject(FunctionExpression functionExp, IFunction function, List<string> fields)
        {
            var objectBody = "";
            foreach (var fn in fields)
            {
                if (objectBody != "")
                    objectBody += ", ";
                objectBody += fn + ": d." + fn;
            }
            var newMapCode = "d => { return {" + objectBody + "}; }";
            var res1 = EngineExV8.ExecuteExprWithReset(newMapCode, "newMapCode");

            var functionObject = new JsHandleV8(ref res1);
            return functionObject;
        }

        public override void SetArgs()
        {
           //noop
        }


        public JavaScriptMapOperationV8(AbstractJavaScriptIndexV8 index, JavaScriptIndexUtils<JsHandleV8> jsIndexUtils, FunctionInstance mapFuncJint, JsHandleV8 mapFunc, string indexName, string mapString)
            : base(index, jsIndexUtils, mapFuncJint, mapFunc, indexName, mapString)
        {
            IndexV8 = index;
        }
    }

    public class JavaScriptMapOperationJint : JavaScriptMapOperation<JsHandleJint>
    {
        private JintEngineEx EngineExJint => _engineHandle as JintEngineEx;

        protected override JsHandleJint CreateFunctionObject(FunctionExpression functionExp, IFunction function, List<string> fields)
        {
            var functionObject = new JsHandleJint(new ScriptFunctionInstance(
                EngineForParsing,
                functionExp,
                EngineForParsing.CreateNewDeclarativeEnvironment(),
                function.Strict
            ));
            return functionObject;
        }

        public override void SetArgs()
        {
            _resolver.ExplodeArgsOn(null, null);
        }

        protected readonly JintPreventResolvingTasksReferenceResolver _resolver;

        public JavaScriptMapOperationJint(AbstractJavaScriptIndexJint index, JavaScriptIndexUtils<JsHandleJint> jsIndexUtils, FunctionInstance mapFuncJint, JsHandleJint mapFunc, string indexName, string mapString) 
            : base(index, jsIndexUtils, mapFuncJint, mapFunc, indexName, mapString)
        {
            _resolver = index.EngineEx.RefResolver;
        }
    }

    public abstract class JavaScriptMapOperation<T> 
        where T : struct, IJsHandle<T>
    {
        protected readonly AbstractJavaScriptIndex<T> _index;
        private readonly JavaScriptIndexUtils<T> _jsIndexUtils;
        protected readonly IJsEngineHandle<T> _engineHandle;
        private JavaScriptEngineType _jsEngineType => _engineHandle.EngineType;
        protected Engine EngineForParsing { get; }
        protected readonly Engine _engineStaticJint;

        public FunctionInstance MapFuncJint;
        public T MapFunc;


        public bool HasDynamicReturns;

        public bool HasBoostedFields;

        public HashSet<string> Fields = new HashSet<string>();
        public Dictionary<string, IndexFieldOptions> FieldOptions = new Dictionary<string, IndexFieldOptions>();
        public string IndexName { get; set; }
        protected JavaScriptMapOperation(AbstractJavaScriptIndex<T> index, JavaScriptIndexUtils<T> jsIndexUtils, FunctionInstance mapFuncJint, T mapFunc, string indexName, string mapString)
        {
            _index = index;
            EngineForParsing = jsIndexUtils.EngineForParsing;
            _engineStaticJint = EngineForParsing;

            _jsIndexUtils = jsIndexUtils;
            _engineHandle = _jsIndexUtils.EngineHandle;

            MapFunc = mapFunc;
            MapFuncJint = mapFuncJint ?? throw new ArgumentNullException(nameof(mapFuncJint));
            IndexName = indexName;
            MapString = mapString;
        }

        protected abstract T CreateFunctionObject(FunctionExpression functionExp, IFunction function, List<string> fields);
        public IEnumerable<T> IndexingFunction(IEnumerable<object> items)
        {
            var memorySnapshotName = "map";
            bool isMemorySnapshotMade = false;
            if (_engineHandle.IsMemoryChecksOn)
            {
                _engineHandle.MakeSnapshot(memorySnapshotName);
                isMemorySnapshotMade = true;
            }

            foreach (var item in items)
            {
                _engineHandle.ResetCallStack();
                _engineHandle.ResetConstraints();

                if (_jsIndexUtils.GetValue(item, out T jsItem) == false)
                    continue;

                if (jsItem.IsObject)
                {
                    using (jsItem)
                    {
                        T jsRes = _engineHandle.Empty;
                        try
                        {
                            if (!MapFunc.IsFunction)
                            {
                                throw new JavaScriptIndexFuncException($"MapFunc is not a function");
                            }

                            jsRes = MapFunc.StaticCall(jsItem);
                            //TODO: egor add exception handling?
                            //if (_index._lastException != null)
                            //{
                            //    ExceptionDispatchInfo.Capture(_index._lastException).Throw();
                            //}
                            //else
                            //{
                            jsRes.ThrowOnError();
                            //}
                        }
                        catch (JavaScriptException jse)
                        {
                            ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                            var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(MapString, jse);
                            if (success == false)
                                throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", jse);
                            throw new JavaScriptIndexFuncException($"Failed to execute map script, {message}", jse);
                        }
                        catch (V8Exception jse)
                        {
                            ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                            var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(MapString, jse);
                            if (success == false)
                                throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", jse);
                            throw new JavaScriptIndexFuncException($"Failed to execute map script, {message}", jse);
                        }
                        catch (Exception e)
                        {
                            ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                            throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", e);
                        }
                        finally
                        {
                            //  _index._lastException = null;
                        }

                        using (jsRes)
                        {
                            if (jsRes.IsArray)
                            {
                                var length = (uint)jsRes.ArrayLength;
                                for (int i = 0; i < length; i++)
                                {
                                    var arrItem = jsRes.GetProperty(i);
                                    using (arrItem)
                                    {
                                        if (arrItem.IsObject)
                                        {
                                            yield return arrItem; // being yield it is converted to blittable object and not disposed - so disposing it here
                                        }
                                        else
                                        {
                                            ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);

                                            // this check should be to catch map errors
                                            throw new JavaScriptIndexFuncException($"Failed to execute {MapString}",
                                                new Exception($"At least one of map results is not object: {jsRes.ToString()}"));
                                        }
                                    }
                                }
                            }
                            else if (jsRes.IsObject)
                            {
                                yield return jsRes; // being yield it is converted to blittable object and not disposed - so disposing it here
                            }
                            // we ignore everything else by design, we support only
                            // objects and arrays, anything else is discarded
                        }
                    }

                    _engineHandle.ForceGarbageCollection();
                    if (isMemorySnapshotMade)
                    {
                        _engineHandle.CheckForMemoryLeaks(memorySnapshotName, shouldRemove: false);
                    }
                }
                else
                {
                    using (jsItem)
                        throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", new Exception($"Entry item is not document: {jsItem.ToString()}"));
                }

                // TODO [shlomo] why there is no SetArgs in indexes?
                SetArgs();
            }

            if (isMemorySnapshotMade)
            {
                _engineHandle.RemoveMemorySnapshot(memorySnapshotName);
            }
        }

        public abstract void SetArgs();
        private void ProcessRunException(T jsRes, string memorySnapshotName, bool isMemorySnapshotMade)
        {
            jsRes.Dispose();

            _engineHandle.ForceGarbageCollection();
            if (isMemorySnapshotMade)
            {
                _engineHandle.CheckForMemoryLeaks(memorySnapshotName);
            }
        }
        
        public void Analyze(Engine engine)
        {
            HasDynamicReturns = false;
            HasBoostedFields = false;

            IFunction theFuncAst;
            switch (MapFuncJint)
            {
                case ScriptFunctionInstance sfi:
                    theFuncAst = sfi.FunctionDeclaration;
                    break;

                default:
                    return;
            }

            var res = CheckIfSimpleMapExpression(engine, theFuncAst);
            if (res != null)
            {
                MapFunc.Set(res.Value.Function);
                theFuncAst = res.Value.FunctionAst;
            }

            foreach (var returnStatement in JavaScriptIndexUtils<T>.GetReturnStatements(theFuncAst))
            {
                if (returnStatement.Argument == null) // return;
                    continue;

                switch (returnStatement.Argument)
                {
                    case ObjectExpression oe:

                        //If we got here we must validate that all return statements have the same structure.
                        //Having zero fields means its the first return statements we encounter that has a structure.
                        if (Fields.Count == 0)
                        {
                            foreach (var prop in oe.Properties)
                            {
                                if (prop is Property property)
                                {
                                    var fieldName = property.GetKey(engine);
                                    var fieldNameAsString = fieldName.AsString();
                                    if (fieldName == "_")
                                        HasDynamicReturns = true;

                                    Fields.Add(fieldNameAsString);

                                    var fieldValue = property.Value;
                                    if (IsBoostExpression(fieldValue))
                                        HasBoostedFields = true;
                                }
                            }
                        }
                        else if (CompareFields(oe) == false)
                        {
                            throw new InvalidOperationException($"Index {IndexName} contains different return structure from different code paths," +
                                                                $" expected properties: {string.Join(", ", Fields)} but also got:{string.Join(", ", oe.Properties.OfType<IProperty>().Select(x => x.GetKey(engine)))}");
                        }

                        break;

                    case CallExpression ce:

                        if (IsBoostExpression(ce))
                            HasBoostedFields = true;
                        else
                            HasDynamicReturns = true;

                        break;

                    default:
                        HasDynamicReturns = true;
                        break;
                }
            }

            static bool IsBoostExpression(Node expression)
            {
                return expression is CallExpression ce && ce.Callee is Identifier identifier && identifier.Name == "boost";
            }
        }

        protected  (T Function, IFunction FunctionAst)? CheckIfSimpleMapExpression(Engine engine, IFunction function)
        {
            var field = function.TryGetFieldFromSimpleLambdaExpression();
            if (field == null)
                return null;
            Identifier self = new Identifier("self");
            var properties = new List<Node>
            {
                new Property(PropertyKind.Init, new Identifier(field), false,
                    new StaticMemberExpression(self, new Identifier(field), optional: false), false, false)
            };
            List<string> fields = new List<string>();
            fields.Add(field);

            if (MoreArguments != null)
            {
                for (uint i = 0; i < MoreArguments.Length; i++)
                {
                    var arg = MoreArguments[i].As<FunctionInstance>();
                    if (arg is not ScriptFunctionInstance sfi)
                        continue;
                    var moreFuncAst = sfi.FunctionDeclaration;
                    field = moreFuncAst.TryGetFieldFromSimpleLambdaExpression();
                    if (field != null)
                    {
                        properties.Add(new Property(PropertyKind.Init, new Identifier(field), false,
                            new StaticMemberExpression(self, new Identifier(field), optional: false), false, false));
                        fields.Add(field);
                    }
                }
            }

            var functionExp = new FunctionExpression(
                function.Id,
                NodeList.Create(new List<Node> { self }),
                new BlockStatement(NodeList.Create(new List<Statement>
                {
                    new ReturnStatement(new ObjectExpression(NodeList.Create(properties)))
                })),
                generator: false,
                function.Strict,
                async: false);

            return (CreateFunctionObject(functionExp, function, fields), functionExp);
        }
        

        public JsArray MoreArguments { get; set; }
        public string MapString { get; internal set; }

        protected bool CompareFields(ObjectExpression oe)
        {
            if (Fields.Count != oe.Properties.Count)
                return false;
            foreach (var p in oe.Properties)
            {
                if (p is IProperty property)
                {
                    var key = property.GetKey(_engineStaticJint);
                    var keyAsString = key.AsString();
                    if (Fields.Contains(keyAsString) == false)
                        return false;
                }
            }

            return true;
        }
    }
}
