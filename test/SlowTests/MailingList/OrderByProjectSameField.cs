// -----------------------------------------------------------------------
//  <copyright file="OrderByProjectSameField.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class OrderByProjectSameField : RavenTestBase
    {
        private class CategoryIndex : AbstractMultiMapIndexCreationTask<CategoryIndexResult>
        {
            public CategoryIndex()
            {
                AddMap<Product>(docs => from doc in docs
                                        where !doc.Id.Contains("-")
                                        select new
                                        {
                                            CategoryId = doc.CategoryId,
                                            Name = (string)null,
                                            Count = 1
                                        });

                AddMap<Category>(docs => from doc in docs select new { CategoryId = doc.Id, Name = doc.Name, Count = 0 });

                Reduce = results => from result in results
                                    group result by result.CategoryId
                                        into g
                                    select new CategoryIndexResult
                                    {
                                        CategoryId = g.Key,
                                        Name = g.Select(x => x.Name).DefaultIfEmpty("Unassigned Products").FirstOrDefault(x => x != null),
                                        Count = g.Sum(x => x.Count)
                                    };

                Store(x => x.Name, FieldStorage.Yes);
            }
        }

        private class CategoryIndexResult
        {
            public string CategoryId { get; set; }

            public string Name { get; set; }

            public int Count { get; set; }
        }

        private class Category
        {
            public Category()
            {
                CustomerEnabled = true;
            }

            #region Properties

            public string Id { get; set; }

            public string Description { get; set; }

            public bool CustomerEnabled { get; set; }

            public string Name { get; set; }

            public int Ordinal { get; set; }

            public int? ParentId { get; set; }

            public int ProductCount { get; set; }

            public int ProductsOutstanding { get; set; }

            public string Unspsc { get; set; }

            #endregion
        }

        [DebuggerDisplay("Id: {Id} Code: {Code} CategoryId: {CategoryId} ")]
        private class Product
        {
            private string id;

            //private string rsId = ConfigurationManager.AppSettings["RSComp1ProductId"];

            private string groupId;

            private string categoryId;

            private IList<string> imagesId = new List<string>();

            public Product()
            {
                AssignedTags = new List<string>();
                Categories = new string[0];
                imagesId = new List<string>();
            }

            public string Id
            {
                get
                {
                    return id;
                }

                set
                {
                    id = value;
                }
            }

            public bool CustomerEnabled { get; set; }

            public string Description { get; set; }

            public string CategoryId
            {
                get
                {
                    return categoryId;
                }

                set
                {
                    categoryId = value;
                    GroupId = null;
                }
            }

            [Description("Generated by the system, this contains the entire category breadcrumb back to the root")]
            public string[] Categories { get; set; }

            public bool Component { get; set; }

            public bool Dangerous { get; set; }

            public bool Discontinued { get; set; }

            public bool Hazardous { get; set; }

            public bool HazardousArea { get; set; }

            public double LeadTime { get; set; }

            [Obsolete("Manufacturer object now in product, Id has been deprecated, use code's instead", true)]
            public string ManufacturerId { get; set; }

            public string Code { get; set; }

            public string Name { get; set; }

            public bool? RoHS { get; set; }

            public string SupplierCode { get; set; }

            public string UNSPSC { get; set; }

            public double Weight { get; set; }

            public bool Inactive { get; set; }

            public string GroupId
            {
                get
                {
                    return groupId;
                }

                set
                {
                    groupId = value;
                    this.categoryId = null;
                }
            }

            public IList<string> AssignedTags { get; set; }

            public string[] Images
            {
                get
                {
                    return imagesId.ToArray();
                }

                private set
                {
                    imagesId = value.ToList();
                }
            }

            public void AddImage(string imageId)
            {
                if (!imagesId.Contains(imageId))
                    imagesId.Add(imageId);
            }

            public void RemoveImage(string imageId)
            {
                imagesId.Remove(imageId);
            }

            public void ClearImages()
            {
                imagesId.Clear();
            }
        }

        private List<Category> Categories { get; set; }

        private List<Product> Products { get; set; }


        [Fact]
        public void FindCategoryByName_Works()
        {
            using (var store = GetDocumentStore())
            {
                new CategoryIndex().Execute(store);

                const string categories = "[{\"Id\":null,\"Description\":\"Description1\",\"CustomerEnabled\":true,\"Name\":\"Name1\",\"Ordinal\":1,\"ParentId\":1,\"ProductCount\":1,\"ProductsOutstanding\":1,\"Unspsc\":\"Unspsc1\"},{\"Id\":null,\"Description\":\"Description2\",\"CustomerEnabled\":true,\"Name\":\"Name2\",\"Ordinal\":2,\"ParentId\":2,\"ProductCount\":2,\"ProductsOutstanding\":2,\"Unspsc\":\"Unspsc2\"},{\"Id\":null,\"Description\":\"Description3\",\"CustomerEnabled\":true,\"Name\":\"Name3\",\"Ordinal\":3,\"ParentId\":3,\"ProductCount\":3,\"ProductsOutstanding\":3,\"Unspsc\":\"Unspsc3\"},{\"Id\":null,\"Description\":\"Description4\",\"CustomerEnabled\":true,\"Name\":\"Name4\",\"Ordinal\":4,\"ParentId\":4,\"ProductCount\":4,\"ProductsOutstanding\":4,\"Unspsc\":\"Unspsc4\"},{\"Id\":null,\"Description\":\"Description5\",\"CustomerEnabled\":true,\"Name\":\"Name5\",\"Ordinal\":5,\"ParentId\":5,\"ProductCount\":5,\"ProductsOutstanding\":5,\"Unspsc\":\"Unspsc5\"}]";
                Categories = JsonConvert.DeserializeObject<List<Category>>(categories);

                const string products = "[{\"Id\":null,\"CustomerEnabled\":false,\"Description\":\"Description1\",\"CategoryId\":null,\"Categories\":[],\"Component\":false,\"Dangerous\":false,\"Discontinued\":false,\"Hazardous\":false,\"HazardousArea\":false,\"LeadTime\":1.0,\"ManufacturerId\":\"ManufacturerId1\",\"Code\":\"Code1\",\"Name\":\"Name1\",\"RoHS\":false,\"SupplierCode\":\"SupplierCode1\",\"UNSPSC\":\"UNSPSC1\",\"Weight\":1.0,\"Inactive\":false,\"GroupId\":\"GroupId1\",\"AssignedTags\":[],\"Images\":[]},{\"Id\":null,\"CustomerEnabled\":true,\"Description\":\"Description2\",\"CategoryId\":null,\"Categories\":[],\"Component\":true,\"Dangerous\":true,\"Discontinued\":true,\"Hazardous\":true,\"HazardousArea\":true,\"LeadTime\":2.0,\"ManufacturerId\":\"ManufacturerId2\",\"Code\":\"Code2\",\"Name\":\"Name2\",\"RoHS\":true,\"SupplierCode\":\"SupplierCode2\",\"UNSPSC\":\"UNSPSC2\",\"Weight\":2.0,\"Inactive\":true,\"GroupId\":\"GroupId2\",\"AssignedTags\":[],\"Images\":[]},{\"Id\":null,\"CustomerEnabled\":false,\"Description\":\"Description3\",\"CategoryId\":null,\"Categories\":[],\"Component\":false,\"Dangerous\":false,\"Discontinued\":false,\"Hazardous\":false,\"HazardousArea\":false,\"LeadTime\":3.0,\"ManufacturerId\":\"ManufacturerId3\",\"Code\":\"Code3\",\"Name\":\"Name3\",\"RoHS\":false,\"SupplierCode\":\"SupplierCode3\",\"UNSPSC\":\"UNSPSC3\",\"Weight\":3.0,\"Inactive\":false,\"GroupId\":\"GroupId3\",\"AssignedTags\":[],\"Images\":[]},{\"Id\":null,\"CustomerEnabled\":true,\"Description\":\"Description4\",\"CategoryId\":null,\"Categories\":[],\"Component\":true,\"Dangerous\":true,\"Discontinued\":true,\"Hazardous\":true,\"HazardousArea\":true,\"LeadTime\":4.0,\"ManufacturerId\":\"ManufacturerId4\",\"Code\":\"Code4\",\"Name\":\"Name4\",\"RoHS\":true,\"SupplierCode\":\"SupplierCode4\",\"UNSPSC\":\"UNSPSC4\",\"Weight\":4.0,\"Inactive\":true,\"GroupId\":\"GroupId4\",\"AssignedTags\":[],\"Images\":[]},{\"Id\":null,\"CustomerEnabled\":false,\"Description\":\"Description5\",\"CategoryId\":null,\"Categories\":[],\"Component\":false,\"Dangerous\":false,\"Discontinued\":false,\"Hazardous\":false,\"HazardousArea\":false,\"LeadTime\":5.0,\"ManufacturerId\":\"ManufacturerId5\",\"Code\":\"Code5\",\"Name\":\"Name5\",\"RoHS\":false,\"SupplierCode\":\"SupplierCode5\",\"UNSPSC\":\"UNSPSC5\",\"Weight\":5.0,\"Inactive\":false,\"GroupId\":\"GroupId5\",\"AssignedTags\":[],\"Images\":[]}]";
                Products = JsonConvert.DeserializeObject<List<Product>>(products);

                using (var session = store.OpenSession())
                {
                    foreach (var category in Categories)
                    {
                        session.Store(category);
                    }
                    foreach (var product in Products)
                    {
                        session.Store(product);
                    }

                    session.SaveChanges();
                }

                var expected = Categories.First().Name;

                using (var session = store.OpenSession())
                {
                    var actual = CategoryNotWorking(session, expected);
                    Assert.Single(actual, x => x == expected);
                }
            }
        }

        private string[] CategoryNotWorking(IDocumentSession session, string text, int count = 8)
        {
            text = TextWildcard(text);

            var query =
                session.Query<Category, CategoryIndex>()
                       .Customize(x => x.WaitForNonStaleResults())
                       .Search(x => x.Name, text, 1, SearchOptions.Or, EscapeQueryOptions.AllowPostfixWildcard)
                       .OrderBy(x => x.Name)
                       .Select(x => x.Name);

            var results = query.ToList();

            var category = query.ToArray();
            return category.Distinct().Take(count).ToArray();
        }

        private string[] CategoryWorking(IDocumentSession session, string text, int count = 8)
        {
            text = TextWildcard(text);

            var query =
                session.Query<Category, CategoryIndex>()
                       .Customize(x => x.WaitForNonStaleResults())
                       .Search(x => x.Name, text, 1, SearchOptions.Or, EscapeQueryOptions.AllowPostfixWildcard)
                       .OrderBy(x => x.Name);

            var results = query.ToList();

            var category = query.ToArray();
            return category.Select(x => x.Name).Distinct().Take(count).ToArray();
        }

        private static string TextWildcard(string text)
        {
            return text.EndsWith("*") ? text : text + "*";
        }
    }
}
