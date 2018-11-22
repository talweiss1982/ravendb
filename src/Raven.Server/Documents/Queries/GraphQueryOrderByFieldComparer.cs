using System;
using System.Collections.Generic;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    internal class GraphQueryOrderByFieldComparer: IComparer<GraphQueryRunner.Match>
    {
        private OrderByField _field;
        private string _alias;
        private BlittablePath _path;

        public GraphQueryOrderByFieldComparer(OrderByField field)
        {
            var fieldName = field.Name.Value;
            var indexOfDot = fieldName.IndexOf('.');
            if (indexOfDot < 0)
                throw new NotSupportedException($"{GetType().Name} got an order by field: {fieldName} that isn't in the expected format of alias.fieldName");
            _alias = fieldName.Substring(0, indexOfDot);
            _path = new BlittablePath(fieldName.Substring(indexOfDot + 1, fieldName.Length - indexOfDot - 1));
            _field = field;
        }



        public int Compare(GraphQueryRunner.Match x, GraphQueryRunner.Match y)
        {
            int order = _field.Ascending ? 1 : -1;
            object xObject = null;
            object yObject = null;
            var xResult = x.GetResult(_alias);
            var yResult = y.GetResult(_alias);
            switch (xResult)
            {
                case Document xDocument:
                    if (yResult is Document yDocument)
                    {
                        xDocument.EnsureMetadata();
                        yDocument.EnsureMetadata();
                        xObject = _path.Evaluate(xDocument.Data, true);
                        yObject = _path.Evaluate(yDocument.Data, true);
                        break;
                    }
                    ThrowMissmatchTypes(xResult, yResult);                   
                    break;
                case BlittableJsonReaderObject bjroX:
                    if (yResult is BlittableJsonReaderObject bjroY)
                    {
                        xObject = _path.Evaluate(bjroX, true);
                        yObject = _path.Evaluate(bjroY, true);
                        break;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case LazyStringValue xLazyStringValue:
                    if (yResult is LazyStringValue yLazyStringValue)
                    {
                        xObject = xLazyStringValue;
                        yObject = yLazyStringValue;
                    }
                    else if(yResult is string yStringValue)
                    {
                        xObject = xLazyStringValue;
                        yObject = yStringValue;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case string xString:
                    if (yResult is string yString)
                    {
                        xObject = xString;
                        yObject = yString;
                    } else if (yResult is LazyStringValue yLazyStringValue2)
                    {
                        xObject = xString;
                        yObject = yLazyStringValue2;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case LazyNumberValue xLazyNumberValue:
                    xObject = xLazyNumberValue;
                    switch (yResult)
                    {
                        case int i:
                            yObject = i;
                            break;
                        case long l:
                            yObject = l;
                            break;
                        case double d:
                            yObject = d;
                            break;
                        case float f:
                            yObject = f;
                            break;
                        case LazyNumberValue lnv:
                            yObject = lnv;
                            break;
                        default:
                            ThrowMissmatchTypes(xResult, yResult);
                            break;
                    }                                        
                    break;
                case int xInt:
                    if (yResult is int yInt)
                    {
                        xObject = xInt;
                        yObject = yInt;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case long xLong:
                    if (yResult is long yLong)
                    {
                        xObject = xLong;
                        yObject = yLong;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case double xDouble:
                    if (yResult is double yDouble)
                    {
                        xObject = xDouble;
                        yObject = yDouble;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                default:
                    throw new NotSupportedException($"Got unexpected types for compare ${xResult} of type {xResult.GetType().Name} and ${yResult} of type {yResult.GetType().Name}.");
            }

            switch (xObject)
            {
                case LazyStringValue xLazyStringValue:
                    if (yObject is LazyStringValue yLazyStringValue)
                    {
                        return xLazyStringValue.CompareTo(yLazyStringValue) * order;
                    }
                    else if (yResult is string yStringValue)
                    {
                        return xLazyStringValue.CompareTo(yStringValue) * order;
                    }
                    ThrowMissmatchTypes(xObject, yObject);
                    break;
                case string xString:
                    if (yObject is string yString)
                    {
                        return string.CompareOrdinal(xString, yString) * order;
                    }
                    ThrowMissmatchTypes(xObject, yObject);
                    break;
                case LazyNumberValue xLazyNumberValue:
                    switch (yObject)
                    {
                        case int i:
                            return xLazyNumberValue.CompareTo(i) * order;
                        case long l:
                            return xLazyNumberValue.CompareTo(l) * order;
                        case double d:
                            return xLazyNumberValue.CompareTo(d) * order;
                        case float f:
                            return xLazyNumberValue.CompareTo(f) * order;
                        case LazyNumberValue lnv:
                            return xLazyNumberValue.CompareTo(lnv) * order;
                        default:
                            ThrowMissmatchTypes(xObject, yObject);
                            break;
                    }
                    break;
                case long xLong:
                    if (yObject is long yLong)
                    {
                        return xLong.CompareTo(yLong) * order;
                    }
                    ThrowMissmatchTypes(xObject, yObject);
                    break;
                case IComparable xComparable:
                    return xComparable.CompareTo(yObject) * order;
                default:
                    ThrowMissmatchTypes(xObject, yObject);
                    break;
            }
            //we never really get here but just in case its better to throw.
            ThrowMissmatchTypes(xObject, yObject);
            return 0;
        }

        private void ThrowMissmatchTypes(object xResult, object yResult)
        {
            throw new NotSupportedException($"Got unexpected types for compare ${xResult} of type {xResult.GetType().Name} and ${yResult} of type {yResult.GetType().Name}.");
        }
    }

    internal class GraphQueryMultipleFieldsComparer : IComparer<GraphQueryRunner.Match>
    {
        private List<GraphQueryOrderByFieldComparer> _comparers;

        public GraphQueryMultipleFieldsComparer(IEnumerable<OrderByField> fields)
        {
            _comparers = new List<GraphQueryOrderByFieldComparer>();
            foreach (var field in fields)
            {
                _comparers.Add(new GraphQueryOrderByFieldComparer(field));
            }
        }

        public int Compare(GraphQueryRunner.Match x, GraphQueryRunner.Match y)
        {
            foreach (var comparer in _comparers)
            {
                var res = comparer.Compare(x, y);
                if (res != 0)
                    return res;
            }

            return 0;
        }
    }
}
