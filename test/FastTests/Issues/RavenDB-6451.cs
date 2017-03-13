﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyModel;
using Raven.Client.Documents.Exceptions;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6451
    {
        private HashSet<Assembly> _assemblies = new HashSet<Assembly>();

        private IEnumerable<Assembly> GetAssemblies(Assembly assemblyToScan)
        {
            if (_assemblies.Add(assemblyToScan) == false)
                yield break;

            yield return assemblyToScan;

            foreach (var referencedAssembly in assemblyToScan.GetReferencedAssemblies())
            {
                Assembly assembly;
                try
                {
                    assembly = Assembly.Load(referencedAssembly);
                }
                catch (Exception)
                {
                    continue;
                }

                foreach (var asm in GetAssemblies(assembly))
                    yield return asm;
            }
        }

        [Fact]
        public void Exceptions_should_not_have_blittable_and_pointer_fields()
        {
            //sanity check
            Assert.True(HasInvalidProperties(typeof(ClassWithPointer)));
            Assert.True(HasInvalidProperties(typeof(ClassWithNestedPointer)));
            Assert.True(HasInvalidProperties(typeof(ClassWithBlittable)));
            Assert.True(HasInvalidProperties(typeof(ClassWithNestedClassWithBlittable)));

            var referenceAssemblies = GetAssemblies(GetType().GetTypeInfo().Assembly);
            var exceptionTypes =
                (from type in referenceAssemblies.SelectMany(x => x.ExportedTypes)
                    where typeof(Exception).IsAssignableFrom(type)
                    select type).ToArray();

            foreach (var t in exceptionTypes)
            {
                try
                {
                    Assert.False(HasInvalidProperties(t), $"The type {t.FullName} should not have pointer or BlittableXXX properties");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        public bool HasInvalidProperties(Type t)
        {
            var properties = t.GetProperties();
            foreach (var p in properties)
            {
                if (p.PropertyType.IsPointer)
                    return true;

                if (p.PropertyType.Name.Contains("Blittable"))
                    return true;

                if (p.PropertyType.IsArray ||
                    p.PropertyType.Namespace.StartsWith("System") ||
                    p.PropertyType.GetTypeInfo().IsEnum ||
                    p.PropertyType.GetTypeInfo().IsPrimitive)
                {
                    continue;
                }

                if (p.PropertyType.GetTypeInfo().IsPrimitive == false &&
                    IsIEnumerable(p.PropertyType) == false &&
                    p.PropertyType.GetTypeInfo().IsInterface == false &&
                    p.PropertyType.GetTypeInfo().ContainsGenericParameters == false &&
                    typeof(IEnumerable).IsAssignableFrom(p.PropertyType) == false &&
                    HasInvalidProperties(p.PropertyType))
                {
                    return true;
                }
            }        

            return false;
        }

        public bool IsIEnumerable(Type t)
        {
            if (t.IsConstructedGenericType)
            {
                var genericType = t.GetGenericTypeDefinition();
                if(genericType.GetInterfaces().Any(x => x.IsConstructedGenericType && 
                                                        x.GetGenericTypeDefinition() == typeof(IEnumerable<>)))                    
                    return true;
            }
            return false;
        }

        public unsafe class ClassWithPointer
        {
            public List<string> JustAList { get; set; }

            public byte* Ptr { get; set; }
        }

        public class ClassWithNestedPointer
        {
            public ClassWithPointer CPtr { get; set; }
        }

        public class ClassWithBlittable
        {
            public BlittableJsonReaderObject Json { get; set; }
        }

        public class ClassWithNestedClassWithBlittable
        {
            public ClassWithBlittable CJson { get; set; }
        }
    }
}
