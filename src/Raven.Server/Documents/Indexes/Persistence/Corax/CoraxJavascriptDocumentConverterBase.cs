using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Sparrow.Json;
using Sparrow.Server;
using System.Linq;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Utils;
using CoraxLib = global::Corax;
using Jint.Native;
using Google.Protobuf.WellKnownTypes;
using Esprima;
using Jint;
using Lucene.Net.Documents;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public abstract class CoraxJavascriptDocumentConverterBase<T> : CoraxDocumentConverterBase
    where T : struct, IJsHandle<T>
{
    protected readonly IJsEngineHandle<T> EngineHandle;

    protected CoraxJavascriptDocumentConverterBase(Index index, IndexDefinition indexDefinition, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries,
        int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null)
        : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
            keyFieldName, storeValueFieldName, fields)
    {
        EngineHandle = ((AbstractJavaScriptIndex<T>)index._compiled).EngineHandle;
        indexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    protected abstract object GetBlittableSupportedType(T val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext);

    //TODO: egor this seems to be same as LuceneJavascriptDocumentConverterBase
    public override ByteStringContext<ByteStringMemoryCache>.InternalScope SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object documentObj, JsonOperationContext indexContext,
        out LazyStringValue id, out ByteString output)
    {
        // We prepare for the next entry.
        ref var entryWriter = ref GetEntriesWriter();
        if (documentObj is not T documentToProcess)
        {
            //nothing to index, finish the job
            id = null;
            entryWriter.Finish(out output);
            return default;
        }

        if (documentToProcess.IsObject == false)
        {
            id = null;
            output = default;
            return default;
        }

        id = key ?? (sourceDocumentId ?? throw new InvalidDataException("Cannot find any identifier of the document."));
        var singleEntryWriterScope = new SingleEntryWriterScope(Allocator);

        if (LuceneJavascriptDocumentConverterBase<T>.TryGetBoostedValue(documentToProcess, EngineHandle, out var boostedValue, out var documentBoost))
        {
            boostedValue.Dispose();
            ThrowWhenBoostingIsInDocument();
        }
        //Write id/key
        singleEntryWriterScope.Write(string.Empty, 0, id.AsSpan(), ref entryWriter);
        var indexingScope = CurrentIndexingScope.Current;
        foreach (var (propertyName, actualVal) in documentToProcess.GetOwnProperties())
        {
            object value;
            var field = GetFieldObjectForProcessing(propertyName);
            var isDynamicFieldEnumerable = IsDynamicFieldEnumerable(actualVal, propertyName, field, out var iterator);
            bool shouldSaveAsBlittable;

            using (actualVal)
            {
                if (isDynamicFieldEnumerable)
                {
                    var enumerableScope = CreateEnumerableWriterScope();
                    enumerableScope.SetAsDynamic();
                    do
                    {
                        ProcessObject(iterator.Current, propertyName, field, ref entryWriter, enumerableScope, out shouldSaveAsBlittable, out value, out var actualValue);
                        if (shouldSaveAsBlittable)
                            ProcessAsJson(actualValue, field, ref entryWriter, enumerableScope);

                        var disposable = value as IDisposable;
                        disposable?.Dispose();

                    } while (iterator.MoveNext());

                    enumerableScope.Finish(field.Name, field.Id, ref entryWriter);
                }
                else
                {
                    ProcessObject(actualVal, propertyName, field, ref entryWriter, singleEntryWriterScope, out shouldSaveAsBlittable, out value,
                        out var actualValue);
                    if (shouldSaveAsBlittable)
                        ProcessAsJson(actualValue, field, ref entryWriter, singleEntryWriterScope);
                    var disposable = value as IDisposable;
                    disposable?.Dispose();
                }
            }

        }

        if (_storeValue)
        {
            StoreValue(ref entryWriter, singleEntryWriterScope);
        }

        return entryWriter.Finish(out output);

        void ProcessAsJson(T actualValue, IndexField field, ref CoraxLib.IndexEntryWriter entryWriter, IWriterScope writerScope)
        {
            var value = GetBlittableSupportedType(actualValue, flattenArrays: false, forIndexing: true, indexContext);
            InsertRegularField(field, value, indexContext, ref entryWriter, writerScope);
        }

        void ProcessObject(T valueToInsert, in string propertyAsString, IndexField field, ref CoraxLib.IndexEntryWriter entryWriter, IWriterScope writerScope, out bool shouldProcessAsBlittable, out object value, out T actualValue)
        {
            value = null;
            actualValue = valueToInsert;
            var isObject = LuceneJavascriptDocumentConverterBase<T>.IsObject(actualValue);
            if (isObject)
            {
                if (LuceneJavascriptDocumentConverterBase<T>.TryGetBoostedValue(actualValue, EngineHandle, out boostedValue, out _))
                {
                    boostedValue.Dispose();
                    //todo leftover to implement boosting inside index someday, now it will throw
                }

                //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                //so we must use field.Name and not property from this point on.

                //LuceneJavascriptDocumentConverterBase<T>.TryDetectDynamicFieldCreation(propertyAsString, EngineHandle, iterator.Current, field);
                using var val = LuceneJavascriptDocumentConverterBase<T>.TryDetectDynamicFieldCreation(propertyAsString, EngineHandle, actualValue, field, indexingScope);
                if (val.IsEmpty == false)
                {
                    if (val.IsObject && val.HasProperty(SpatialPropertyName))
                    {
                        actualValue = val; //Here we populate the dynamic spatial field that will be handled below.
                    }
                    else
                    {
                        value = GetBlittableSupportedType(val, flattenArrays: false, forIndexing: true, indexContext);

                        InsertRegularField(field, value, indexContext, ref entryWriter, writerScope);

                        if (value is IDisposable toDispose1)
                        {
                            // the value was converted to a corax field and isn't needed anymore
                            toDispose1.Dispose();
                        }

                        shouldProcessAsBlittable = false;
                        return;
                    }
                }

                if (actualValue.HasOwnProperty(SpatialPropertyName) && actualValue.TryGetValue(SpatialPropertyName, out var inner))
                {
                    using (inner)
                    {
                        SpatialField spatialField;
                        IEnumerable<object> spatial;
                        if (inner.IsStringEx)
                        {
                            spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                            spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString);
                        }
                        else if (inner.IsObject)
                        {
                            if (inner.HasOwnProperty("Lat") && inner.HasOwnProperty("Lng") && inner.TryGetValue("Lat", out var lat))
                            {
                                using (lat)
                                {
                                    if (lat.IsNumberOrIntEx && inner.TryGetValue("Lng", out var lng) && lng.IsNumberOrIntEx)
                                    {
                                        using (lng)
                                        {
                                            spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                            spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsDouble,
                                                lng.AsDouble);
                                        }
                                    }
                                    else
                                    {
                                        shouldProcessAsBlittable = false;
                                        return; //Ignoring bad spatial field
                                    }
                                }
                            }
                            else
                            {
                                shouldProcessAsBlittable = false;
                                return; //Ignoring bad spatial field
                            }
                        }
                        else
                        {
                            shouldProcessAsBlittable = false;
                            return; //Ignoring bad spatial field
                        }

                        InsertRegularField(field, spatial, indexContext, ref entryWriter, writerScope);

                        shouldProcessAsBlittable = false;
                        return;
                    }
                }
            }

            shouldProcessAsBlittable = true;
        }

        IndexField GetFieldObjectForProcessing(in string propertyAsString)
        {
            if (_fields.TryGetValue(propertyAsString, out var field) == false)
            {
                int currentId = CoraxLib.Constants.IndexWriter.DynamicField;
                if (KnownFieldsForWriter.TryGetByFieldName(Allocator, propertyAsString, out var binding))
                    currentId = binding.FieldId;

                field = _fields[propertyAsString] = IndexField.Create(propertyAsString, new IndexFieldOptions(), _allFields, currentId);
                indexingScope.DynamicFields ??= new();
                indexingScope.DynamicFields[propertyAsString] = field.Indexing;
                indexingScope.CreatedFieldsCount++;
            }

            return field;
        }

        bool IsDynamicFieldEnumerable(T propertyDescriptorValue, string propertyAsString, IndexField field, out IEnumerator<T> iterator)
        {
            iterator = Enumerable.Empty<T>().GetEnumerator();

            if (propertyDescriptorValue.IsArray == false)
                return false;
            
            int arrayLength = propertyDescriptorValue.ArrayLength;
            var jsItems = new List<T>();
            for (int i = 0; i < arrayLength; i++)
            {
                jsItems[i] = propertyDescriptorValue.GetProperty(i);
            }

            iterator = jsItems.GetEnumerator();
            if (iterator.MoveNext() == false || iterator.Current.IsNull || iterator.Current.IsObject == false || iterator.Current.IsArray == true)
                return false;


            using var x = LuceneJavascriptDocumentConverterBase<T>.TryDetectDynamicFieldCreation(propertyAsString, EngineHandle, iterator.Current, field, indexingScope);
            return x.IsNull == false || iterator.Current.HasOwnProperty(SpatialPropertyName);
        }

        void StoreValue(ref CoraxLib.IndexEntryWriter entryWriter, SingleEntryWriterScope scope)
        {
            var storedValue = JsBlittableBridge<T>.Translate(indexContext, scriptEngine: EngineHandle, objectInstance: documentToProcess);
            unsafe
            {
                using (Allocator.Allocate(storedValue.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        storedValue.CopyTo(bPtr);

                    scope.Write(string.Empty, GetKnownFieldsForWriter().Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }
    }
    protected static void ThrowWhenBoostingIsInDocument()
    {
        throw new NotImplementedInCoraxException("Indexing-time boosting is not implemented.");
    }
    public override void Dispose()
    {
        base.Dispose();
    }
}
