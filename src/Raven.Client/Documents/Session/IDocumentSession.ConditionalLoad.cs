﻿//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session
    /// </summary>
    public partial interface IDocumentSession
    {
        /// <summary>
        ///     Conditional load the specified entity with the specified id and changeVector.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be conditional loaded.</param>
        /// <param name="changeVector">Change vector of a entity that will be conditional loaded.</param>
        (T Entity, string ChangeVector) ConditionalLoad<T>(string id, string changeVector);
    }
}
