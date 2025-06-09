using System;
using Raven.Client.Documents.Attachments;
using Sparrow.Json;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    public class MissingAttachmentException : RavenException
    {

        public MissingAttachmentException()
        {
        }


        public MissingAttachmentException(string message) : base(message)
        {
        }

        public MissingAttachmentException(string message, Exception inner) : base(message, inner)
        {
        }

        public static MissingAttachmentException ThrowForAttachment(string documentId, string attachmentName, string attachmentHash, AttachmentType type)
        {
            throw new MissingAttachmentException($"Attachment table entry '{attachmentName}' with hash '{attachmentHash}' does not exist for type '{type}' with id '{documentId}' .");
        }


        public static MissingAttachmentException ThrowForHash(string documentId, string attachmentName, string attachmentHash, AttachmentType type)
        {
            throw new MissingAttachmentException($"Attachment stream for '{attachmentName}' with hash '{attachmentHash}' does not exist for type '{type}' with id '{documentId}' .");
        }
    }
}
